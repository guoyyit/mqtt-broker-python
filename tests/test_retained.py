"""Requirement 4: retained messages."""

from __future__ import annotations

from tests.mqtt_client import MQTTTestClient


def _client(port, cid, **kw):
    c = MQTTTestClient("127.0.0.1", port)
    c.connect(cid, username="user", password="pass", **kw)
    return c


def test_retained_delivered_on_new_subscription(broker):
    pub = _client(broker.port, "pub")
    pub.publish("r/topic", b"retained!", qos=1, retain=True)
    pub.expect_packet(4)  # PUBACK

    sub = _client(broker.port, "sub")
    sub.subscribe(("r/#", 1))
    topic, payload, qos, retain, _, _ = sub.read_publish()
    assert (topic, payload, retain) == ("r/topic", b"retained!", True)
    assert qos == 1  # min(retained qos 1, subscription qos 1)
    sub.disconnect()
    pub.disconnect()


def test_retained_only_latest_per_topic(broker):
    pub = _client(broker.port, "pub")
    pub.publish("r/one", b"v1", retain=True)
    pub.publish("r/one", b"v2", retain=True)
    sub = _client(broker.port, "sub")
    sub.subscribe(("r/one", 0))
    assert sub.read_publish()[1] == b"v2"
    assert sub.try_read_packet(timeout=0.3) is None
    sub.disconnect()
    pub.disconnect()


def test_empty_payload_clears_retained(broker):
    pub = _client(broker.port, "pub")
    pub.publish("r/clr", b"data", retain=True)
    pub.publish("r/clr", b"", retain=True)  # clear
    sub = _client(broker.port, "sub")
    sub.subscribe(("r/clr", 0))
    assert sub.try_read_packet(timeout=0.3) is None
    sub.disconnect()
    pub.disconnect()


def test_retained_matches_wildcard_filters(broker):
    pub = _client(broker.port, "pub")
    pub.publish("w/a", b"A", retain=True)
    pub.publish("w/x/b", b"B", retain=True)
    pub.publish("other", b"C", retain=True)
    sub = _client(broker.port, "sub")
    sub.subscribe(("w/+", 0))
    assert sub.read_publish()[1] == b"A"
    assert sub.try_read_packet(timeout=0.3) is None  # w/x/b needs two levels
    sub2 = _client(broker.port, "sub2")
    sub2.subscribe(("w/#", 0))
    got = {sub2.read_publish()[1] for _ in range(2)}
    assert got == {b"A", b"B"}
    sub.disconnect()
    sub2.disconnect()
    pub.disconnect()


def test_retained_forwarded_to_live_subscribers_with_retain_flag(broker):
    sub = _client(broker.port, "sub")
    sub.subscribe(("live/#", 0))
    pub = _client(broker.port, "pub")
    pub.publish("live/t", b"L", retain=True)
    _, payload, _, retain, _, _ = sub.read_publish()
    assert payload == b"L" and retain
    sub.disconnect()
    pub.disconnect()
