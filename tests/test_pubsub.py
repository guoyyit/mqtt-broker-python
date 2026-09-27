"""Requirement 2: wildcard subscriptions and online-only delivery."""

from __future__ import annotations

from tests.mqtt_client import MQTTTestClient


def _client(port, cid, **kw):
    c = MQTTTestClient("127.0.0.1", port)
    c.connect(cid, username="user", password="pass", **kw)
    return c


def test_basic_publish_subscribe(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    assert sub.subscribe(("a/b", 0)) == [0]
    pub.publish("a/b", b"hello")
    topic, payload, qos, retain, dup, _ = sub.read_publish()
    assert (topic, payload, qos, retain, dup) == ("a/b", b"hello", 0, False, False)
    sub.disconnect()
    pub.disconnect()


def test_single_level_wildcard(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("home/+/temp", 0))
    pub.publish("home/kitchen/temp", b"21")
    assert sub.read_publish()[1] == b"21"
    # '+' matches exactly one level: this must NOT be delivered
    pub.publish("home/a/b/temp", b"x")
    pub.publish("home/temp", b"y")
    assert sub.try_read_packet(timeout=0.3) is None
    sub.disconnect()
    pub.disconnect()


def test_multi_level_wildcard(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("sensors/#", 0))
    pub.publish("sensors/a", b"1")
    pub.publish("sensors/a/b/c", b"2")
    assert sub.read_publish()[1] == b"1"
    assert sub.read_publish()[1] == b"2"
    # "sensors/#" also matches the parent level itself
    pub.publish("sensors", b"3")
    assert sub.read_publish()[1] == b"3"
    # non-matching branch is not delivered
    pub.publish("actuators/a", b"9")
    assert sub.try_read_packet(timeout=0.3) is None
    sub.disconnect()
    pub.disconnect()


def test_wildcard_does_not_match_system_topics(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("#", 0))
    pub.publish("$SYS/broker/uptime", b"1")
    assert sub.try_read_packet(timeout=0.3) is None
    sub.disconnect()
    pub.disconnect()


def test_offline_client_does_not_receive(broker):
    sub = _client(broker.port, "sub")
    sub.subscribe(("t/#", 0))
    sub.disconnect()  # clean session -> subscription gone
    pub = _client(broker.port, "pub")
    pub.publish("t/x", b"m")
    late = _client(broker.port, "late")
    late.subscribe(("t/#", 0))
    assert late.try_read_packet(timeout=0.3) is None
    pub.disconnect()
    late.disconnect()


def test_unsubscribe_stops_delivery(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("u/topic", 0))
    sub.unsubscribe("u/topic")
    pub.publish("u/topic", b"m")
    assert sub.try_read_packet(timeout=0.3) is None
    sub.disconnect()
    pub.disconnect()


def test_invalid_filter_rejected_in_suback(broker):
    sub = _client(broker.port, "sub")
    assert sub.subscribe(("a/#/b", 1)) == [0x80]
    sub.disconnect()
