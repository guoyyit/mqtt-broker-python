"""纯 Python 异步 MQTT 3.1.1 客户端，仅用于本地回环自动化测试。

精确控制报文与连接行为（可正常 DISCONNECT 或直接掐断 TCP），不引入第三方
MQTT 依赖。内部使用一个后台读任务把报文分发到：

- 下行 PUBLISH 队列（:meth:`recv_publish`）；
- 控制报文 future 表（PUBACK/PUBREC/PUBREL/PUBCOMP/SUBACK/UNSUBACK/PINGRESP），
  使发布确认与接收消息之间不存在读取顺序耦合。
"""

from __future__ import annotations

import asyncio
import collections
import uuid
from typing import Optional

from . import packet


class MQTTTestClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 1883) -> None:
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.client_id: Optional[str] = None
        self._next_pid_out = 1
        self._closed = False
        self._incoming: asyncio.Queue = asyncio.Queue()
        # packet_id -> Future，用于等待 PUBREC/PUBCOMP 等入站 QoS 握手。
        self._waiters: dict[tuple[int, int], asyncio.Future] = {}
        self._reader_task: Optional[asyncio.Task] = None
        self._read_error: Optional[Exception] = None

    def _alloc_pid(self) -> int:
        pid = self._next_pid_out
        self._next_pid_out = 1 if pid == 65535 else pid + 1
        return pid

    def _write(self, data: bytes) -> None:
        assert self.writer is not None
        self.writer.write(data)

    async def _reader(self) -> None:
        try:
            while True:
                header, body = await packet.read_packet(self.reader)
                ptype = header.packet_type
                if ptype == packet.PacketType.PUBLISH:
                    await self._incoming.put((header, body))
                    continue
                pid = int.from_bytes(body[:2], "big") if len(body) >= 2 else 0
                # PINGRESP 无报文标识符，固定键 (PINGRESP, 0)。
                key = (int(ptype), pid)
                waiter = self._waiters.pop(key, None)
                if waiter is not None and not waiter.done():
                    waiter.set_result((header, body))
        except (asyncio.IncompleteReadError, ConnectionError,
                BrokenPipeError, RuntimeError) as exc:
            self._read_error = exc
        except Exception as exc:  # 协议错误等
            self._read_error = exc
        finally:
            # 唤醒所有等待者。
            for waiter in self._waiters.values():
                if not waiter.done():
                    waiter.set_exception(ConnectionError("connection closed"))

    async def _expect(self, ptype: int, pid: int, timeout: float = 3.0):
        key = (ptype, pid)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._waiters[key] = future
        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self._waiters.pop(key, None)

    async def connect(self, client_id: str | None = None, *, clean_session: bool = True,
                      keep_alive: int = 0, username: Optional[str] = None,
                      password: Optional[str | bytes] = None,
                      will_topic: Optional[str] = None, will_payload: bytes = b"",
                      will_qos: int = 0, will_retain: bool = False
                      ) -> tuple[bool, bool]:
        """返回 (session_present, accepted)。"""
        if client_id is None:
            client_id = f"test-{uuid.uuid4().hex[:8]}"
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self._incoming = asyncio.Queue()
        self._waiters = {}
        self._read_error = None
        self.client_id = client_id

        flags = 0x02 if clean_session else 0
        payload = bytearray(packet.encode_string(client_id))
        if will_topic is not None:
            flags |= 0x04 | (will_qos << 3) | (0x20 if will_retain else 0)
            payload += packet.encode_string(will_topic)
            payload += packet.encode_bytes_value(will_payload)
        if username is not None:
            flags |= 0x80
            payload += packet.encode_string(username)
        if password is not None:
            flags |= 0x40
            raw = password.encode("utf-8") if isinstance(password, str) else password
            payload += packet.encode_bytes_value(raw)

        variable = (packet.encode_string("MQTT") + bytes([4, flags])
                    + keep_alive.to_bytes(2, "big"))
        self._write(packet.encode_packet(packet.CONNECT, bytes(variable) + bytes(payload)))
        await self.writer.drain()
        header, body = await asyncio.wait_for(packet.read_packet(self.reader), 5)
        if header.packet_type != packet.PacketType.CONNACK or len(body) != 2:
            raise packet.ProtocolError("expected CONNACK")
        self._closed = False
        self._reader_task = asyncio.create_task(self._reader())
        return bool(body[0] & 0x01), body[1] == 0

    async def publish(self, topic: str, payload: bytes = b"", *, qos: int = 0,
                      retain: bool = False) -> Optional[int]:
        if qos == 0:
            self._write(packet.encode_publish(packet.PublishPacket(
                topic=topic, payload=payload, qos=0, retain=retain)))
            await self.writer.drain()
            return None
        pid = self._alloc_pid()
        self._write(packet.encode_publish(packet.PublishPacket(
            topic=topic, payload=payload, qos=qos, retain=retain, packet_id=pid)))
        await self.writer.drain()
        if qos == 1:
            await self._expect(packet.PUBACK, pid)
        else:
            await self._expect(packet.PUBREC, pid)
            self._write(packet.encode_ack(packet.PUBREL, pid))
            await self.writer.drain()
            await self._expect(packet.PUBCOMP, pid)
        return pid

    async def raw_publish(self, topic: str, payload: bytes, *, qos: int,
                          packet_id: int, dup: bool = False,
                          retain: bool = False) -> None:
        """直接发送一帧 PUBLISH（不处理任何确认），供重传/去重测试。"""
        self._write(packet.encode_publish(packet.PublishPacket(
            topic=topic, payload=payload, qos=qos, retain=retain,
            dup=dup, packet_id=packet_id)))
        await self.writer.drain()

    async def subscribe(self, topics, qos: int = 1) -> list[int]:
        """topics 可为字符串列表，或 (filter, qos) 列表。"""
        entries = []
        for item in topics if isinstance(topics, (list, tuple)) else [topics]:
            entries.append(item if isinstance(item, tuple) else (item, qos))
        pid = self._alloc_pid()
        variable = pid.to_bytes(2, "big")
        for topic_filter, wanted_qos in entries:
            variable += packet.encode_string(topic_filter) + bytes([wanted_qos])
        self._write(packet.encode_packet(packet.SUBSCRIBE, variable, flags=0x02))
        await self.writer.drain()
        _, body = await self._expect(packet.SUBACK, pid)
        return list(body[2:])

    async def unsubscribe(self, topics) -> None:
        if isinstance(topics, str):
            topics = [topics]
        pid = self._alloc_pid()
        variable = pid.to_bytes(2, "big")
        for topic_filter in topics:
            variable += packet.encode_string(topic_filter)
        self._write(packet.encode_packet(packet.UNSUBSCRIBE, variable, flags=0x02))
        await self.writer.drain()
        await self._expect(packet.UNSUBACK, pid)

    async def recv_publish(self, timeout: float = 2.0, *, auto_ack: bool = True
                           ) -> tuple[str, bytes, int, bool, Optional[int]]:
        """读取一条下行 PUBLISH，自动完成 QoS1/2 的客户端确认握手。

        返回 (topic, payload, qos, retain, packet_id)。
        auto_ack=False 时不发送任何确认，供测试重传/恢复边界。
        """
        header, body = await asyncio.wait_for(self._incoming.get(), timeout)
        pub = packet.parse_publish(header, body)
        if auto_ack and pub.qos == 1:
            self._write(packet.encode_ack(packet.PUBACK, pub.packet_id))
            await self.writer.drain()
        elif auto_ack and pub.qos == 2:
            self._write(packet.encode_ack(packet.PUBREC, pub.packet_id))
            await self.writer.drain()
            await self._expect(packet.PUBREL, pub.packet_id)
            self._write(packet.encode_ack(packet.PUBCOMP, pub.packet_id))
            await self.writer.drain()
        return pub.topic, pub.payload, pub.qos, pub.retain, pub.packet_id

    async def complete_qos2(self, packet_id: int) -> None:
        """auto_ack=False 收到 QoS2 后，手动完成 PUBREC/PUBREL/PUBCOMP。"""
        self._write(packet.encode_ack(packet.PUBREC, packet_id))
        await self.writer.drain()
        await self._expect(packet.PUBREL, packet_id)
        self._write(packet.encode_ack(packet.PUBCOMP, packet_id))
        await self.writer.drain()

    async def ack_qos1(self, packet_id: int) -> None:
        self._write(packet.encode_ack(packet.PUBACK, packet_id))
        await self.writer.drain()

    async def ping(self) -> None:
        pid = 0
        self._write(packet.encode_packet(packet.PINGREQ, b""))
        # PINGRESP 无 packet id，单独等待。
        key = (packet.PINGRESP, 0)
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._waiters[key] = future
        await self.writer.drain()
        try:
            await asyncio.wait_for(future, 3)
        finally:
            self._waiters.pop(key, None)

    async def disconnect(self) -> None:
        if self.writer is not None and not self._closed:
            self._write(packet.encode_packet(packet.DISCONNECT, b""))
            await self.writer.drain()
            await self._shutdown_writer()

    async def hard_disconnect(self) -> None:
        """直接关闭 TCP（不发 DISCONNECT），模拟异常断开/崩溃。"""
        if self.writer is not None:
            import socket
            import struct
            transport = self.writer.transport
            sock = transport.get_extra_info("socket")
            if sock is not None:
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                    struct.pack("ii", 1, 0))
                except OSError:
                    pass
            await self._shutdown_writer()

    async def _shutdown_writer(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, BrokenPipeError, RuntimeError):
            pass
        self._closed = True
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await asyncio.wait_for(self._reader_task, 1)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
