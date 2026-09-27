"""MQTT 3.1.1 控制报文的编解码。

仅实现 broker 与测试客户端需要的部分；所有解析在遇到违反协议的字节时抛出
ProtocolError，由上层决定关闭连接（MQTT 3.1.1 §3.1.4 / §4.8）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Awaitable, Callable, Optional

# 固定报文类型
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


class PacketType(IntEnum):
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


class ConnectReturnCode(IntEnum):
    ACCEPTED = 0
    UNACCEPTABLE_PROTOCOL = 1
    IDENTIFIER_REJECTED = 2
    SERVER_UNAVAILABLE = 3
    BAD_USERNAME_PASSWORD = 4
    NOT_AUTHORIZED = 5


class QoS(IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2
    FAILURE = 0x80


@dataclass
class FixedHeader:
    packet_type: PacketType
    flags: int = 0
    remaining_length: int = 0


@dataclass
class ConnectPacket:
    client_id: str = ""
    clean_session: bool = True
    keep_alive: int = 0
    username: Optional[str] = None
    password: Optional[bytes] = None
    will_topic: Optional[str] = None
    will_message: bytes = b""
    will_qos: int = 0
    will_retain: bool = False


@dataclass
class PublishPacket:
    topic: str = ""
    payload: bytes = b""
    qos: int = 0
    retain: bool = False
    dup: bool = False
    packet_id: Optional[int] = None


@dataclass
class SubscribePacket:
    packet_id: int = 0
    topics: list[tuple[str, int]] = field(default_factory=list)


@dataclass
class UnsubscribePacket:
    packet_id: int = 0
    topics: list[str] = field(default_factory=list)


class ProtocolError(Exception):
    """报文违反 MQTT 3.1.1 协议规范。"""


# ---------------------------------------------------------------------------
# 基础字段
# ---------------------------------------------------------------------------

def encode_remaining_length(length: int) -> bytes:
    if length < 0 or length > 268_435_455:
        raise ProtocolError("remaining length out of range")
    out = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


async def decode_remaining_length(read_exact: Callable[[int], Awaitable[bytes]]) -> int:
    multiplier = 1
    value = 0
    for _ in range(4):
        byte = (await read_exact(1))[0]
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            return value
        multiplier *= 128
    raise ProtocolError("malformed remaining length")


def encode_string(value: str) -> bytes:
    raw = value.encode("utf-8")
    if len(raw) > 0xFFFF:
        raise ProtocolError("string too long")
    return len(raw).to_bytes(2, "big") + raw


def encode_bytes_value(value: bytes) -> bytes:
    if len(value) > 0xFFFF:
        raise ProtocolError("binary field too long")
    return len(value).to_bytes(2, "big") + value


def decode_string(data: bytes, offset: int) -> tuple[str, int]:
    raw, offset = decode_binary(data, offset)
    try:
        return raw.decode("utf-8"), offset
    except UnicodeDecodeError as exc:
        raise ProtocolError("invalid UTF-8 string") from exc


def decode_binary(data: bytes, offset: int) -> tuple[bytes, int]:
    if offset + 2 > len(data):
        raise ProtocolError("truncated length prefix")
    length = int.from_bytes(data[offset:offset + 2], "big")
    offset += 2
    if offset + length > len(data):
        raise ProtocolError("truncated field")
    return data[offset:offset + length], offset + length


# ---------------------------------------------------------------------------
# 出站编码
# ---------------------------------------------------------------------------

def encode_packet(packet_type: int, variable: bytes, flags: int = 0) -> bytes:
    return bytes([(packet_type << 4) | (flags & 0x0F)]) + encode_remaining_length(len(variable)) + variable


def encode_connack(session_present: bool, return_code: int) -> bytes:
    variable = bytes([1 if session_present else 0, return_code])
    return encode_packet(CONNACK, variable)


def encode_publish(packet: PublishPacket) -> bytes:
    variable = bytearray(encode_string(packet.topic))
    if packet.qos > 0:
        if not packet.packet_id:
            raise ProtocolError("QoS>0 PUBLISH requires packet id")
        variable += int(packet.packet_id).to_bytes(2, "big")
    variable += packet.payload
    flags = (packet.qos & 0x03) << 1
    if packet.dup:
        flags |= 0x08
    if packet.retain:
        flags |= 0x01
    return encode_packet(PUBLISH, bytes(variable), flags)


def encode_ack(packet_type: int, packet_id: int) -> bytes:
    # PUBREL 固定头保留位必须为 0b0010（MQTT 3.1.1 §3.6.1）。
    flags = 0x02 if packet_type == PUBREL else 0x00
    return encode_packet(packet_type, packet_id.to_bytes(2, "big"), flags)


def encode_suback(packet_id: int, return_codes: list[int]) -> bytes:
    return encode_packet(SUBACK, packet_id.to_bytes(2, "big") + bytes(return_codes))


def encode_unsuback(packet_id: int) -> bytes:
    return encode_packet(UNSUBACK, packet_id.to_bytes(2, "big"))


def encode_pingresp() -> bytes:
    return encode_packet(PINGRESP, b"")


# ---------------------------------------------------------------------------
# 入站解析
# ---------------------------------------------------------------------------

async def read_packet(reader) -> tuple[FixedHeader, bytes]:
    first = await reader.readexactly(1)
    byte = first[0]
    ptype = byte >> 4
    flags = byte & 0x0F
    remaining = await decode_remaining_length(reader.readexactly)
    body = await reader.readexactly(remaining) if remaining else b""
    if not 1 <= ptype <= 14:
        raise ProtocolError(f"unknown packet type {ptype}")
    return FixedHeader(PacketType(ptype), flags, remaining), body


def parse_connect(variable: bytes) -> ConnectPacket:
    try:
        protocol_name, offset = decode_string(variable, 0)
    except ProtocolError:
        raise
    if protocol_name != "MQTT":
        raise ProtocolError("unsupported protocol name")
    if offset + 4 > len(variable):
        raise ProtocolError("truncated CONNECT variable header")
    protocol_level = variable[offset]
    if protocol_level != 4:  # MQTT 3.1.1
        raise ProtocolError(f"unsupported protocol level {protocol_level}")
    connect_flags = variable[offset + 1]
    keep_alive = int.from_bytes(variable[offset + 2:offset + 4], "big")
    offset += 4

    flag_username = bool(connect_flags & 0x80)
    flag_password = bool(connect_flags & 0x40)
    will_retain = bool(connect_flags & 0x20)
    will_qos = (connect_flags & 0x18) >> 3
    will_flag = bool(connect_flags & 0x04)
    clean_session = bool(connect_flags & 0x02)
    reserved = connect_flags & 0x01

    if reserved:
        raise ProtocolError("CONNECT reserved bit must be zero")
    if will_qos not in (0, 1, 2):
        raise ProtocolError("invalid will QoS")
    if will_flag is False and (will_retain or will_qos):
        raise ProtocolError("will fields set without will flag")
    if flag_password and not flag_username:
        raise ProtocolError("password without username")

    packet = ConnectPacket(clean_session=clean_session, keep_alive=keep_alive,
                           will_qos=will_qos, will_retain=will_retain)

    packet.client_id, offset = decode_string(variable, offset)
    if packet.client_id == "" and not clean_session:
        # 3.1.1：空 client id 必须同时设置 clean session，否则拒绝（rc=2）。
        raise IdentifierRejected()
    if will_flag:
        packet.will_topic, offset = decode_string(variable, offset)
        packet.will_message, offset = decode_binary(variable, offset)
    if flag_username:
        packet.username, offset = decode_string(variable, offset)
    if flag_password:
        packet.password, offset = decode_binary(variable, offset)
    if offset != len(variable):
        raise ProtocolError("trailing bytes in CONNECT")
    return packet


class IdentifierRejected(ProtocolError):
    """空 ClientID 且 clean session=0，应以 CONNACK rc=2 拒绝。"""


def parse_publish(header: FixedHeader, variable: bytes) -> PublishPacket:
    topic, offset = decode_string(variable, 0)
    dup = bool(header.flags & 0x08)
    qos = (header.flags & 0x06) >> 1
    retain = bool(header.flags & 0x01)
    if qos == 3:
        raise ProtocolError("invalid QoS in PUBLISH")
    packet_id: Optional[int] = None
    if qos > 0:
        if offset + 2 > len(variable):
            raise ProtocolError("truncated PUBLISH packet id")
        packet_id = int.from_bytes(variable[offset:offset + 2], "big")
        offset += 2
        if packet_id == 0:
            raise ProtocolError("packet id must be non-zero")
    elif dup:
        # DUP 只允许在 QoS>0 时置位（§3.3.1.1）。
        raise ProtocolError("DUP flag set on QoS0 PUBLISH")
    return PublishPacket(topic=topic, payload=variable[offset:], qos=qos,
                         retain=retain, dup=dup, packet_id=packet_id)


def parse_packet_id(variable: bytes) -> int:
    if len(variable) != 2:
        raise ProtocolError("packet id field must be 2 bytes")
    packet_id = int.from_bytes(variable, "big")
    if packet_id == 0:
        raise ProtocolError("packet id must be non-zero")
    return packet_id


def parse_subscribe(variable: bytes) -> SubscribePacket:
    packet_id = parse_packet_id(variable[:2])
    offset = 2
    topics: list[tuple[str, int]] = []
    while offset < len(variable):
        topic_filter, offset = decode_string(variable, offset)
        if offset >= len(variable):
            raise ProtocolError("truncated SUBSCRIBE QoS")
        qos = variable[offset]
        offset += 1
        if qos not in (0, 1, 2):
            raise ProtocolError("invalid SUBSCRIBE QoS byte")
        topics.append((topic_filter, qos))
    if not topics:
        raise ProtocolError("SUBSCRIBE must contain at least one filter")
    return SubscribePacket(packet_id=packet_id, topics=topics)


def parse_unsubscribe(variable: bytes) -> UnsubscribePacket:
    packet_id = parse_packet_id(variable[:2])
    offset = 2
    topics: list[str] = []
    while offset < len(variable):
        topic_filter, offset = decode_string(variable, offset)
        topics.append(topic_filter)
    if not topics:
        raise ProtocolError("UNSUBSCRIBE must contain at least one filter")
    return UnsubscribePacket(packet_id=packet_id, topics=topics)
