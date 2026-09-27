"""Asyncio TCP server speaking MQTT 3.1.1, driven by the Broker core.

Each connection runs one reader coroutine and one writer coroutine.  All
outbound bytes flow through a per-connection FIFO queue, which is what
guarantees in-order delivery per connection; there is no shared writer
state between connections, so a stalled client cannot block others.
"""

from __future__ import annotations

import asyncio
import logging

from .broker import Broker, Connection
from .packet import (
    MalformedPacketError,
    PacketType,
    decode_connect,
    decode_packet_id,
    decode_publish,
    decode_subscribe,
    decode_unsubscribe,
    encode_pingresp,
    read_packet,
)

log = logging.getLogger(__name__)


class MQTTServer:
    """Accepts TCP connections and runs the MQTT protocol per connection."""

    def __init__(self, broker: Broker) -> None:
        self._broker = broker
        self._server: asyncio.Server | None = None
        self._handlers: dict[int, _ClientHandler] = {}

    async def start(self) -> None:
        self._broker.connection_closer = self._close_connection
        self._server = await asyncio.start_server(
            self._handle_client, self._broker.config.host, self._broker.config.port
        )
        addr = self._server.sockets[0].getsockname()
        log.info("MQTT broker listening on %s:%s", *addr)

    @property
    def port(self) -> int:
        assert self._server is not None
        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for handler in list(self._handlers.values()):
            handler.close()
        if self._handlers:
            await asyncio.gather(
                *(h.wait_done() for h in list(self._handlers.values())),
                return_exceptions=True,
            )

    def _close_connection(self, conn: Connection) -> None:
        handler = self._handlers.get(conn.token)
        if handler is not None:
            handler.close()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        handler = _ClientHandler(self._broker, reader, writer)
        self._handlers[handler.id] = handler
        try:
            await handler.run()
        finally:
            self._handlers.pop(handler.id, None)


class _ClientHandler:
    _id_counter = 0

    def __init__(
        self,
        broker: Broker,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        _ClientHandler._id_counter += 1
        self.id = _ClientHandler._id_counter
        self._broker = broker
        self._reader = reader
        self._writer = writer
        self._out: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._conn: Connection | None = None
        self._done = asyncio.Event()
        self._closed = False

    # -- outbound path -------------------------------------------------

    def send(self, data: bytes) -> None:
        if not self._closed:
            self._out.put_nowait(data)

    def close(self) -> None:
        """Flush pending bytes, then close the transport."""
        if not self._closed:
            self._closed = True
            self._out.put_nowait(None)

    async def wait_done(self) -> None:
        await self._done.wait()

    async def _writer_loop(self) -> None:
        try:
            while True:
                data = await self._out.get()
                if data is None:
                    break
                self._writer.write(data)
                await self._writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            try:
                self._writer.close()
            except Exception:  # noqa: BLE001 - closing must never raise
                pass

    # -- inbound path --------------------------------------------------

    async def _read_next(self, keep_alive: int):
        if keep_alive > 0:
            # MQTT 3.1.1: disconnect if nothing arrives within 1.5x keepalive
            return await asyncio.wait_for(read_packet(self._reader), keep_alive * 1.5)
        return await read_packet(self._reader)

    async def run(self) -> None:
        writer_task = asyncio.create_task(self._writer_loop())
        graceful = False
        try:
            header, body = await self._read_next(0)
            if header.packet_type != PacketType.CONNECT:
                return  # first packet must be CONNECT
            connect = decode_connect(body)
            conn = await self._broker.connect(self.send, connect)
            if conn is None:
                return  # CONNACK with error already queued; close after flush
            self._conn = conn
            await self._broker.session_established(conn)

            while True:
                header, body = await self._read_next(connect.keep_alive)
                ptype = header.packet_type
                if ptype == PacketType.PUBLISH:
                    await self._broker.publish(conn, decode_publish(header, body))
                elif ptype == PacketType.PUBACK:
                    await self._broker.puback_received(conn, decode_packet_id(body))
                elif ptype == PacketType.PUBREC:
                    await self._broker.pubrec_received(conn, decode_packet_id(body))
                elif ptype == PacketType.PUBREL:
                    if header.flags != 0x02:
                        raise MalformedPacketError("PUBREL must use flags 0x02")
                    await self._broker.pubrel_received(conn, decode_packet_id(body))
                elif ptype == PacketType.PUBCOMP:
                    await self._broker.pubcomp_received(conn, decode_packet_id(body))
                elif ptype == PacketType.SUBSCRIBE:
                    await self._broker.subscribe(
                        conn, decode_subscribe(header.flags, body)
                    )
                elif ptype == PacketType.UNSUBSCRIBE:
                    await self._broker.unsubscribe(
                        conn, decode_unsubscribe(header.flags, body)
                    )
                elif ptype == PacketType.PINGREQ:
                    self.send(encode_pingresp())
                elif ptype == PacketType.DISCONNECT:
                    graceful = True
                    break
                else:
                    raise MalformedPacketError(f"unexpected packet {ptype!r}")
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.TimeoutError):
            pass  # network drop / keepalive timeout -> abnormal disconnect
        except MalformedPacketError as exc:
            log.warning("malformed packet from client: %s", exc)
        finally:
            if self._conn is not None:
                await self._broker.disconnect(self._conn, graceful)
            self.close()
            await writer_task
            self._done.set()
