"""TCP 接入层：连接生命周期、报文读取循环与保活。

每个连接一个 :class:`Connection` 协程任务；所有跨连接共享状态的变更都交给
Broker 在其异步锁内完成。同一连接的所有下行字节通过 ``write_lock`` 串行化，
保证同一连接内控制报文与消息报文的字节顺序。
"""

from __future__ import annotations

import asyncio
from typing import Optional

from . import packet
from .broker import Broker
from .config import BrokerConfig

_PINGRESP = packet.encode_pingresp()


class Connection:
    def __init__(self, broker: Broker, reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter) -> None:
        self.broker = broker
        self.reader = reader
        self.writer = writer
        peername = writer.get_extra_info("peername")
        self.remote = f"{peername[0]}:{peername[1]}" if peername else "unknown"
        self.client_id: Optional[str] = None
        self.session = None
        self.keep_alive: int = 0
        self.clean_session: bool = True
        # CONNECT 完成前不允许其他报文。
        self.authenticated = False
        self.graceful_close = False
        self.write_lock = asyncio.Lock()
        self._last_activity: float = 0.0

    async def send(self, data: bytes) -> bool:
        """线程/协程安全的下行写。失败返回 False。"""
        async with self.write_lock:
            try:
                self.writer.write(data)
                await self.writer.drain()
                return True
            except (ConnectionError, BrokenPipeError, RuntimeError):
                return False

    async def close(self) -> None:
        async with self.write_lock:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except (RuntimeError, ConnectionError, BrokenPipeError):
                pass

    async def _read_loop(self) -> None:
        while True:
            timeout = None
            if self.authenticated and self.keep_alive > 0:
                # 1.5 倍宽限（MQTT 3.1.1 §3.1.2-10）。
                timeout = self.keep_alive * 1.5
            header, body = await asyncio.wait_for(
                packet.read_packet(self.reader), timeout=timeout)
            ptype = header.packet_type
            if ptype == packet.PacketType.CONNECT:
                if self.authenticated:
                    raise packet.ProtocolError("second CONNECT on same connection")
                connect = packet.parse_connect(body)
                accepted = await self.broker.handle_connect(self, connect)
                if not accepted:
                    self.graceful_close = True
                    await self.close()
                    return
                self.authenticated = True
                continue

            if not self.authenticated:
                raise packet.ProtocolError("packet before CONNECT")

            if ptype == packet.PacketType.PUBLISH:
                pub = packet.parse_publish(header, body)
                await self.broker.handle_publish(self, pub)
            elif ptype in (packet.PacketType.PUBACK, packet.PacketType.PUBREC,
                           packet.PacketType.PUBCOMP):
                pid = packet.parse_packet_id(body)
                handler = {
                    packet.PacketType.PUBACK: self.broker.handle_puback,
                    packet.PacketType.PUBREC: self.broker.handle_pubrec,
                    packet.PacketType.PUBCOMP: self.broker.handle_pubcomp,
                }[ptype]
                await handler(self, pid)
            elif ptype == packet.PacketType.PUBREL:
                if header.flags != 0x02:
                    raise packet.ProtocolError("invalid PUBREL fixed header flags")
                pid = packet.parse_packet_id(body)
                await self.broker.handle_pubrel(self, pid)
            elif ptype == packet.PacketType.SUBSCRIBE:
                if header.flags != 0x02:
                    raise packet.ProtocolError("invalid SUBSCRIBE fixed header flags")
                sub = packet.parse_subscribe(body)
                codes = await self.broker.handle_subscribe(self, sub)
                await self.send(packet.encode_suback(sub.packet_id, codes))
            elif ptype == packet.PacketType.UNSUBSCRIBE:
                if header.flags != 0x02:
                    raise packet.ProtocolError("invalid UNSUBSCRIBE fixed header flags")
                unsub = packet.parse_unsubscribe(body)
                await self.broker.handle_unsubscribe(self, unsub.topics)
                await self.send(packet.encode_unsuback(unsub.packet_id))
            elif ptype == packet.PacketType.PINGREQ:
                if body:
                    raise packet.ProtocolError("PINGREQ must have empty payload")
                await self.send(_PINGRESP)
            elif ptype == packet.PacketType.DISCONNECT:
                if body:
                    raise packet.ProtocolError("DISCONNECT must have empty payload")
                self.graceful_close = True
                return
            else:  # CONNACK / SUBACK 等客户端->服务端方向非法
                raise packet.ProtocolError(f"unexpected client packet {ptype}")

    async def run(self) -> None:
        try:
            # CONNECT 必须在合理时间内到达。
            await asyncio.wait_for(self._read_until_connect(), timeout=10)
            if not self.authenticated:
                return
            await self._read_loop()
        except asyncio.IncompleteReadError:
            pass  # 对端关闭 TCP
        except (asyncio.TimeoutError, ConnectionError, BrokenPipeError):
            pass
        except packet.ProtocolError:
            pass  # 协议违规：静默关闭连接
        except asyncio.CancelledError:
            raise
        finally:
            await self.broker.handle_disconnect(self, graceful=self.graceful_close)
            await self.close()

    async def _read_until_connect(self) -> None:
        """读取首包 CONNECT（含保活超时由外层 10s 限制）。"""
        header, body = await packet.read_packet(self.reader)
        if header.packet_type != packet.PacketType.CONNECT:
            raise packet.ProtocolError("first packet must be CONNECT")
        connect = packet.parse_connect(body)
        accepted = await self.broker.handle_connect(self, connect)
        if not accepted:
            self.graceful_close = True
            return
        self.authenticated = True


class MQTTServer:
    def __init__(self, config: BrokerConfig) -> None:
        self.config = config
        self.broker = Broker(config)
        self._server: Optional[asyncio.AbstractServer] = None
        # 跟踪所有接入协程（含未完成 CONNECT 的），停止时统一取消。
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        await self.broker.start()
        self._server = await asyncio.start_server(
            self._on_connect, host=self.config.host, port=self.config.port,
        )

    async def _accept(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        connection = Connection(self.broker, reader, writer)
        task = asyncio.current_task()
        connection.task = task  # type: ignore[attr-defined]
        await connection.run()

    def _on_connect(self, reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self._accept(reader, writer))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def serve_forever(self) -> None:
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        # 先标记关停：随后中断存量连接属于服务端行为，不得按异常断开发布遗嘱。
        self.broker._stopping = True
        if self._server is not None:
            self._server.close()
            # 立即中断存量连接，避免 wait_closed 等待客户端断开。
            abort = getattr(self._server, "abort_clients", None)
            if abort is not None:
                abort()
            await self._server.wait_closed()
            self._server = None
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.broker.shutdown()
