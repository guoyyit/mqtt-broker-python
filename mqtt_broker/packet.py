"""MQTT 3.1.1 control packet codec (encode / decode)."""

from __future__ import annotations

import enum
import struct
from dataclasses import dataclass


class PacketType(enum.IntEnum):
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14


class ConnectReturnCode(enum.IntEnum):
    ACCEPTED = 0
    UNACCEPTABLE_PROTOCOL_VERSION = 1
    IDENTIFIER_REJECTED = 2
    SERVER_UNAVAILABLE = 3
    BAD_USERNAME_OR_PASSWORD = 4
    NOT_AUTHORIZED = 5


class SubAckReason(enum.IntEnum):
    QOS0 = 0x00
    QOS1 = 0x01
    QOS2 = 0x02
    FAILURE = 0x80


@dataclass(slots=True)
class ConnectPacket:
    client_id: str
    clean_session: bool
    keep_alive: int
    username: str | None
    password: bytes | None
    will_topic: str | None
    will_payload: bytes | None
    will_qos: int
    will_retain: bool


@dataclass(slots=True)
class PublishPacket:
    topic: str
    payload: bytes
    qos: int
    retain: bool
    dup: bool
    packet_id: int | None


@dataclass(slots=True)
class SubscribePacket:
    packet_id: int
    subscriptions: list[tuple[str, int]]  # (filter, requested qos)


@dataclass(slots=True)
class UnsubscribePacket:
    packet_id: int
    filters: list[str]


@dataclass(slots=True)
class FixedHeader:
    packet_type: PacketType
    flags: int
    remaining_length: int


class MalformedPacketError(Exception):
    """Raised when a packet violates the MQTT 3.1.1 wire format."""


# ---------------------------------------------------------------------------
# decoding helpers
# ---------------------------------------------------------------------------

class _Cursor:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def take(self, n: int) -> bytes:
        if self.pos + n > len(self.data):
            raise MalformedPacketError("packet truncated")
        chunk = self.data[self.pos:self.pos + n]
        self.pos += n
        return chunk

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return struct.unpack("!H", self.take(2))[0]

    def binary(self) -> bytes:
        return self.take(self.u16())

    def string(self) -> str:
        raw = self.binary()
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MalformedPacketError("invalid UTF-8 string") from exc

    def at_end(self) -> bool:
        return self.pos == len(self.data)


async def _read_exactly(reader, n: int) -> bytes:
    data = await reader.readexactly(n)
    return data


async def read_packet(reader) -> tuple[FixedHeader, bytes]:
    """Read one packet from an asyncio StreamReader; returns (header, body)."""
    first = await _read_exactly(reader, 1)
    byte1 = first[0]
    packet_type = PacketType(byte1 >> 4)
    flags = byte1 & 0x0F

    multiplier = 1
    remaining_length = 0
    for _ in range(4):
        digit = (await _read_exactly(reader, 1))[0]
        remaining_length += (digit & 0x7F) * multiplier
        if not digit & 0x80:
            break
        multiplier *= 128
    else:
        raise MalformedPacketError("remaining length exceeds 4 bytes")

    body = await _read_exactly(reader, remaining_length)
    return FixedHeader(packet_type, flags, remaining_length), body


def decode_connect(body: bytes) -> ConnectPacket:
    cur = _Cursor(body)
    protocol_name = cur.string()
    if protocol_name != "MQTT":
        raise MalformedPacketError(f"unsupported protocol name {protocol_name!r}")
    protocol_level = cur.u8()
    if protocol_level != 4:
        raise MalformedPacketError(f"unsupported protocol level {protocol_level}")
    connect_flags = cur.u8()
    keep_alive = cur.u16()

    username_flag = bool(connect_flags & 0x80)
    password_flag = bool(connect_flags & 0x40)
    will_retain = bool(connect_flags & 0x20)
    will_qos = (connect_flags >> 3) & 0x03
    will_flag = bool(connect_flags & 0x04)
    clean_session = bool(connect_flags & 0x02)
    if connect_flags & 0x01:
        raise MalformedPacketError("reserved connect flag set")
    if not will_flag and (will_retain or will_qos):
        raise MalformedPacketError("will flags set without will")
    if will_qos == 3:
        raise MalformedPacketError("invalid will qos")

    client_id = cur.string()
    will_topic = will_payload = None
    if will_flag:
        will_topic = cur.string()
        will_payload = cur.binary()
    username = cur.string() if username_flag else None
    password = cur.binary() if password_flag else None
    if not cur.at_end():
        raise MalformedPacketError("trailing bytes in CONNECT")
    return ConnectPacket(
        client_id=client_id,
        clean_session=clean_session,
        keep_alive=keep_alive,
        username=username,
        password=password,
        will_topic=will_topic,
        will_payload=will_payload,
        will_qos=will_qos,
        will_retain=will_retain,
    )


def decode_publish(header: FixedHeader, body: bytes) -> PublishPacket:
    cur = _Cursor(body)
    topic = cur.string()
    qos = (header.flags >> 1) & 0x03
    if qos == 3:
        raise MalformedPacketError("invalid publish qos")
    packet_id = cur.u16() if qos else None
    payload = cur.take(len(body) - cur.pos)
    return PublishPacket(
        topic=topic,
        payload=payload,
        qos=qos,
        retain=bool(header.flags & 0x01),
        dup=bool(header.flags & 0x08),
        packet_id=packet_id,
    )


def decode_subscribe(header_flags: int, body: bytes) -> SubscribePacket:
    if header_flags != 0x02:
        raise MalformedPacketError("SUBSCRIBE must use flags 0x02")
    cur = _Cursor(body)
    packet_id = cur.u16()
    subs: list[tuple[str, int]] = []
    while not cur.at_end():
        topic_filter = cur.string()
        qos = cur.u8()
        if qos > 2:
            raise MalformedPacketError("invalid requested qos")
        subs.append((topic_filter, qos))
    if not subs:
        raise MalformedPacketError("SUBSCRIBE with no subscriptions")
    return SubscribePacket(packet_id=packet_id, subscriptions=subs)


def decode_unsubscribe(header_flags: int, body: bytes) -> UnsubscribePacket:
    if header_flags != 0x02:
        raise MalformedPacketError("UNSUBSCRIBE must use flags 0x02")
    cur = _Cursor(body)
    packet_id = cur.u16()
    filters: list[str] = []
    while not cur.at_end():
        filters.append(cur.string())
    if not filters:
        raise MalformedPacketError("UNSUBSCRIBE with no filters")
    return UnsubscribePacket(packet_id=packet_id, filters=filters)


def decode_packet_id(body: bytes) -> int:
    cur = _Cursor(body)
    packet_id = cur.u16()
    if not cur.at_end():
        raise MalformedPacketError("trailing bytes after packet id")
    if packet_id == 0:
        raise MalformedPacketError("packet id 0 is invalid")
    return packet_id


# ---------------------------------------------------------------------------
# encoding
# ---------------------------------------------------------------------------

def _encode_remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        digit = n % 128
        n //= 128
        if n > 0:
            digit |= 0x80
        out.append(digit)
        if n == 0:
            return bytes(out)


def _string(value: str) -> bytes:
    raw = value.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


def _binary(value: bytes) -> bytes:
    return struct.pack("!H", len(value)) + value


def _packet(packet_type: PacketType, flags: int, body: bytes) -> bytes:
    byte1 = (int(packet_type) << 4) | flags
    return bytes([byte1]) + _encode_remaining_length(len(body)) + body


def encode_connack(session_present: bool, return_code: ConnectReturnCode) -> bytes:
    body = bytes([0x01 if session_present else 0x00, int(return_code)])
    return _packet(PacketType.CONNACK, 0, body)


def encode_publish(packet: PublishPacket) -> bytes:
    flags = (0x08 if packet.dup else 0) | (packet.qos << 1) | (0x01 if packet.retain else 0)
    body = _string(packet.topic)
    if packet.qos:
        assert packet.packet_id is not None
        body += struct.pack("!H", packet.packet_id)
    body += packet.payload
    return _packet(PacketType.PUBLISH, flags, body)


def _encode_packet_id_only(packet_type: PacketType, flags: int, packet_id: int) -> bytes:
    return _packet(packet_type, flags, struct.pack("!H", packet_id))


def encode_puback(packet_id: int) -> bytes:
    return _encode_packet_id_only(PacketType.PUBACK, 0, packet_id)


def encode_pubrec(packet_id: int) -> bytes:
    return _encode_packet_id_only(PacketType.PUBREC, 0, packet_id)


def encode_pubrel(packet_id: int) -> bytes:
    return _encode_packet_id_only(PacketType.PUBREL, 0x02, packet_id)


def encode_pubcomp(packet_id: int) -> bytes:
    return _encode_packet_id_only(PacketType.PUBCOMP, 0, packet_id)


def encode_suback(packet_id: int, reasons: list[SubAckReason]) -> bytes:
    body = struct.pack("!H", packet_id) + bytes(int(r) for r in reasons)
    return _packet(PacketType.SUBACK, 0, body)


def encode_unsuback(packet_id: int) -> bytes:
    return _encode_packet_id_only(PacketType.UNSUBACK, 0, packet_id)


def encode_pingresp() -> bytes:
    return _packet(PacketType.PINGRESP, 0, b"")
