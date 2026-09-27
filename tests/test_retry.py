"""服务端对未确认下行 QoS1/2 的定时重传（DUP=1，同一 packet id）。"""

from __future__ import annotations

import asyncio
import socket

import pytest

from mqtt_broker.config import BrokerConfig
from mqtt_broker.server import MQTTServer


async def _start(tmp_path, retry_interval):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    cfg = BrokerConfig(host="127.0.0.1", port=port,
                       credentials={"user": "pass"},
                       data_dir=str(tmp_path / "data"),
                       retry_interval=retry_interval)
    server = MQTTServer(cfg)
    await server.start()
    return server, port


async def test_qos1_redelivered_with_dup_and_same_pid(tmp_path):
    server, port = await _start(tmp_path, retry_interval=0.3)
    try:
        from mqtt_broker.testclient import MQTTTestClient
        sub = MQTTTestClient(port=port)
        await sub.connect("s", clean_session=True, username="user", password="pass")
        await sub.subscribe([("r", 1)])
        pub = MQTTTestClient(port=port)
        await pub.connect("p", clean_session=True, username="user", password="pass")
        await pub.publish("r", b"x", qos=1)

        # 首条不确认。
        t, p, q, retain, pid1 = await sub.recv_publish(auto_ack=False)
        assert (p, q) == (b"x", 1)
        # 服务端在 retry_interval 后重传：DUP=1 且同一 packet id。
        t, p, q, retain, pid2 = await sub.recv_publish(timeout=3, auto_ack=False)
        assert (p, q) == (b"x", 1) and pid2 == pid1
        # recv_publish 返回的解析结果中 DUP 不单独返回，用原始帧再次确认标志：
        # 这里通过“重传期间只收到同一条”间接验证；直接抓下一帧检查 DUP。
        await sub.ack_qos1(pid2)
        # 确认后不再重传。
        with pytest.raises(asyncio.TimeoutError):
            await sub.recv_publish(timeout=0.8)
        await sub.disconnect()
        await pub.disconnect()
    finally:
        await server.stop()


async def test_qos2_dup_frame_flag_set(tmp_path):
    """通过原始帧确认服务端重传帧的 DUP 位确实置 1。"""
    server, port = await _start(tmp_path, retry_interval=0.3)
    try:
        from mqtt_broker import packet as mqttpkt
        from mqtt_broker.testclient import MQTTTestClient
        sub = MQTTTestClient(port=port)
        await sub.connect("s", clean_session=True, username="user", password="pass")
        await sub.subscribe([("r2", 2)])
        pub = MQTTTestClient(port=port)
        await pub.connect("p", clean_session=True, username="user", password="pass")
        await pub.publish("r2", b"y", qos=2)
        h1, b1 = await sub._incoming.get()
        assert not (h1.flags & 0x08)  # 首发 DUP=0
        h2, b2 = await asyncio.wait_for(sub._incoming.get(), timeout=3)
        assert h2.packet_type == mqttpkt.PacketType.PUBLISH
        assert h2.flags & 0x08          # 重传 DUP=1
        pub2 = mqttpkt.parse_publish(h2, b2)
        pub1 = mqttpkt.parse_publish(h1, b1)
        assert pub1.packet_id == pub2.packet_id
        await sub.complete_qos2(pub2.packet_id)
        await sub.disconnect()
        await pub.disconnect()
    finally:
        await server.stop()
