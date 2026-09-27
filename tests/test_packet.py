"""MQTT 报文编解码边界。"""

from __future__ import annotations

import asyncio

import pytest

from mqtt_broker.packet import (CONNECT, DISCONNECT, PINGREQ, PUBACK, PUBCOMP,
                                PUBREC, PUBREL, SUBSCRIBE, UNSUBSCRIBE,
                                ConnectPacket, FixedHeader, PacketType,
                                ProtocolError, PublishPacket,
                                decode_remaining_length, encode_ack,
                                encode_connack, encode_packet, encode_publish,
                                encode_remaining_length, encode_suback,
                                parse_connect, parse_packet_id, parse_publish,
                                parse_subscribe, parse_unsubscribe)


def _decode_remaining_length_sync(data: bytes) -> int:
    it = iter(data)

    async def read_exact(n):
        return bytes(next(it) for _ in range(n))

    async def run():
        return await decode_remaining_length(read_exact)

    return asyncio.new_event_loop().run_until_complete(run())


def test_remaining_length_boundaries():
    for value in [0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455]:
        assert _decode_remaining_length_sync(encode_remaining_length(value)) == value
    with pytest.raises(ProtocolError):
        encode_remaining_length(268435456)


def test_connack_encoding():
    assert encode_connack(True, 0) == bytes([0x20, 0x02, 0x01, 0x00])
    assert encode_connack(False, 5) == bytes([0x20, 0x02, 0x00, 0x05])


def test_publish_roundtrip_qos0():
    frame = encode_publish(PublishPacket(topic="a/b", payload=b"hi", qos=0))
    header, body = _split(frame)
    pub = parse_publish(header, body)
    assert (pub.topic, pub.payload, pub.qos, pub.retain, pub.dup) == (
        "a/b", b"hi", 0, False, False)


def test_publish_roundtrip_flags_and_pid():
    frame = encode_publish(PublishPacket(
        topic="t", payload=b"p", qos=2, retain=True, dup=True, packet_id=0x1234))
    header, body = _split(frame)
    assert header.flags == 0x0D
    pub = parse_publish(header, body)
    assert (pub.qos, pub.retain, pub.dup, pub.packet_id) == (2, True, True, 0x1234)


def test_acks():
    assert encode_ack(PUBACK, 7) == bytes([0x40, 0x02, 0x00, 0x07])
    assert encode_ack(PUBREL, 7) == bytes([0x62, 0x02, 0x00, 0x07])
    assert parse_packet_id(b"\x00\x09") == 9


def test_parse_connect_full():
    payload = bytearray()
    payload += _s("cid")
    payload += _s("will/t") + b"\x00\x03" + b"bye"
    payload += _s("user") + b"\x00\x03" + b"pw1"
    flags = 0x02 | 0x04 | (1 << 3) | 0x20 | 0x80 | 0x40
    variable = _s("MQTT") + bytes([4, flags]) + (30).to_bytes(2, "big")
    frame = encode_packet(CONNECT, bytes(variable) + bytes(payload))
    header, body = _split(frame)
    connect = parse_connect(body)
    assert connect.client_id == "cid"
    assert connect.clean_session and connect.keep_alive == 30
    assert connect.will_topic == "will/t" and connect.will_message == b"bye"
    assert connect.will_qos == 1 and connect.will_retain
    assert connect.username == "user" and connect.password == b"pw1"


def test_parse_connect_rejects_bad_protocol():
    variable = _s("MQIsdp") + bytes([3, 0x02, 0, 0])
    frame = encode_packet(CONNECT, variable + _s("x"))
    _, body = _split(frame)
    with pytest.raises(ProtocolError):
        parse_connect(body)


def test_parse_connect_empty_id_persistent_rejected():
    flags = 0  # clean=0
    variable = _s("MQTT") + bytes([4, flags, 0, 0])
    frame = encode_packet(CONNECT, variable + _s(""))
    _, body = _split(frame)
    with pytest.raises(ProtocolError):
        parse_connect(body)


def test_subscribe_parsing():
    variable = (1).to_bytes(2, "big") + _s("a/+") + bytes([1]) + _s("b/#") + bytes([2])
    frame = encode_packet(SUBSCRIBE, variable, flags=0x02)
    _, body = _split(frame)
    sub = parse_subscribe(body)
    assert sub.packet_id == 1
    assert sub.topics == [("a/+", 1), ("b/#", 2)]


def test_unsubscribe_parsing():
    variable = (5).to_bytes(2, "big") + _s("x")
    frame = encode_packet(UNSUBSCRIBE, variable, flags=0x02)
    _, body = _split(frame)
    unsub = parse_unsubscribe(body)
    assert unsub.packet_id == 5 and unsub.topics == ["x"]


def test_suback_encoding():
    assert encode_suback(2, [0, 1, 2, 0x80]) == bytes(
        [0x90, 0x06, 0x00, 0x02, 0x00, 0x01, 0x02, 0x80])


def test_publish_invalid_qos3():
    header = FixedHeader(PacketType.PUBLISH, flags=0x06)  # QoS=3
    with pytest.raises(ProtocolError):
        parse_publish(header, _s("t"))


def _s(value: str) -> bytes:
    raw = value.encode()
    return len(raw).to_bytes(2, "big") + raw


def _split(frame: bytes):
    ptype = PacketType(frame[0] >> 4)
    flags = frame[0] & 0x0F
    index = 1
    multiplier = 1
    remaining = 0
    while True:
        byte = frame[index]
        index += 1
        remaining += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            break
        multiplier *= 128
    return FixedHeader(ptype, flags, remaining), frame[index:index + remaining]
