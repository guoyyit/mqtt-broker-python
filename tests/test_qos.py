"""QoS0/1/2 投递边界：降级、顺序、重复与丢失语义。"""

from __future__ import annotations

import asyncio

import pytest

from mqtt_broker.testclient import MQTTTestClient


async def _pair(broker, sub_filters):
    sub = broker.client()
    await sub.connect("sub", clean_session=False, username="user", password="pass")
    await sub.subscribe(sub_filters)
    pub = broker.client()
    await pub.connect("pub", clean_session=True, username="user", password="pass")
    return sub, pub


async def test_qos_downgrade(broker):
    # 订阅 QoS1，发布 QoS2 -> 以 QoS1 投递。
    sub, pub = await _pair(broker, [("t/+", 1)])
    try:
        await pub.publish("t/a", b"x", qos=2)
        t, payload, qos, *_ = await sub.recv_publish()
        assert (t, payload, qos) == ("t/a", b"x", 1)
    finally:
        await sub.disconnect()
        await pub.disconnect()


async def test_in_connection_order_preserved(broker):
    sub, pub = await _pair(broker, [("seq", 2)])
    try:
        for i in range(10):
            await pub.publish("seq", str(i).encode(), qos=(i % 3))
        received = []
        for _ in range(10):
            t, payload, qos, *_ = await sub.recv_publish()
            received.append(int(payload))
        assert received == list(range(10))
    finally:
        await sub.disconnect()
        await pub.disconnect()


async def test_qos2_inbound_dedup_when_publisher_retries(broker):
    """发布者重传同一 packet id 的 QoS2 PUBLISH：订阅者只收到一次。"""
    from mqtt_broker import packet as mqttpkt
    sub, pub = await _pair(broker, [("q2", 2)])
    try:
        pid = pub._alloc_pid()
        await pub.raw_publish("q2", b"once", qos=2, packet_id=pid)
        # 收到首个 PUBREC 后不发 PUBREL，而是重发同一报文。
        await pub._expect(mqttpkt.PUBREC, pid)
        await pub.raw_publish("q2", b"once", qos=2, packet_id=pid, dup=True)
        # 重复 PUBLISH 仍幂等回 PUBREC，但不会再次投递给订阅者。
        await pub._expect(mqttpkt.PUBREC, pid)
        pub._write(mqttpkt.encode_ack(mqttpkt.PUBREL, pid))
        await pub.writer.drain()
        await pub._expect(mqttpkt.PUBCOMP, pid)

        t, payload, qos, *_ = await sub.recv_publish()
        assert payload == b"once"
        with pytest.raises(asyncio.TimeoutError):
            await sub.recv_publish(timeout=0.5)
    finally:
        await sub.disconnect()
        await pub.disconnect()


async def test_unacked_qos1_redelivered_after_reconnect_with_dup(broker):
    """下行 QoS1 未确认即异常断开：重连后按 DUP 重传，且只重传未确认的。"""
    sub = broker.client()
    await sub.connect("q1c", clean_session=False, username="user", password="pass")
    await sub.subscribe([("r", 1)])
    pub = broker.client()
    await pub.connect("p", clean_session=True, username="user", password="pass")
    try:
        await pub.publish("r", b"first", qos=1)
        t, p, q, retain, pid1 = await sub.recv_publish()  # 自动确认
        assert p == b"first"
        await pub.publish("r", b"second", qos=1)
        t, p, q, retain, pid2 = await sub.recv_publish(auto_ack=False)
        assert p == b"second"
        await sub.hard_disconnect()
        await asyncio.sleep(0.2)
        sub2 = broker.client()
        present, accepted = await sub2.connect(
            "q1c", clean_session=False, username="user", password="pass")
        assert accepted and present
        t, p, q, retain, pid3 = await sub2.recv_publish(timeout=2, auto_ack=False)
        assert p == b"second", p          # 已确认的 first 不重传
        assert q == 1
        await sub2.ack_qos1(pid3)
    finally:
        await pub.disconnect()
        await sub2.disconnect()


async def test_clean_session_discards_unacked(broker):
    """clean session 重连不恢复任何旧状态。"""
    sub = broker.client()
    await sub.connect("cleaner", clean_session=True, username="user", password="pass")
    await sub.subscribe([("c", 2)])
    pub = broker.client()
    await pub.connect("p", clean_session=True, username="user", password="pass")
    await pub.publish("c", b"x", qos=2)
    t, p, *_ = await sub.recv_publish(auto_ack=False)
    assert p == b"x"
    await sub.hard_disconnect()
    await asyncio.sleep(0.2)
    sub2 = broker.client()
    present, accepted = await sub2.connect(
        "cleaner", clean_session=True, username="user", password="pass")
    assert accepted and present is False
    await sub2.subscribe([("c", 2)])
    with pytest.raises(asyncio.TimeoutError):
        await sub2.recv_publish(timeout=0.5)
    await sub2.disconnect()
    await pub.disconnect()
