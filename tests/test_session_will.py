"""Requirement 5: persistent sessions and will messages."""

from __future__ import annotations

from tests.mqtt_client import MQTTTestClient


def _client(port, cid, clean=True, **kw):
    c = MQTTTestClient("127.0.0.1", port)
    c.connect(cid, clean=clean, username="user", password="pass", **kw)
    return c


def test_persistent_session_queues_messages_while_offline(broker):
    sub = _client(broker.port, "pers", clean=False)
    assert sub.subscribe(("p/#", 1)) == [1]
    sub.disconnect()  # session stays

    pub = _client(broker.port, "pub")
    pub.publish("p/1", b"m1", qos=1)
    pub.publish("p/2", b"m2", qos=1)
    pub.expect_packet(4)
    pub.expect_packet(4)
    pub.disconnect()

    back = _client(broker.port, "pers", clean=False)
    got = {back.read_publish()[1] for _ in range(2)}
    assert got == {b"m1", b"m2"}
    back.disconnect()


def test_persistent_session_present_flag(broker):
    c1 = _client(broker.port, "flag", clean=False)
    c1.disconnect()
    c2 = MQTTTestClient("127.0.0.1", broker.port)
    sp, rc = c2.connect("flag", clean=False, username="user", password="pass")
    assert rc == 0 and sp == 1  # session present
    c2.disconnect()


def test_clean_session_discards_state(broker):
    sub = _client(broker.port, "cleanme", clean=False)
    sub.subscribe(("c/#", 1))
    sub.disconnect()
    # reconnect with clean_session=True wipes the session
    wipe = _client(broker.port, "cleanme", clean=True)
    wipe.disconnect()
    pub = _client(broker.port, "pub")
    pub.publish("c/x", b"lost", qos=1)
    pub.expect_packet(4)
    back = _client(broker.port, "cleanme", clean=False)
    assert back.try_read_packet(timeout=0.3) is None
    back.disconnect()
    pub.disconnect()


def test_will_published_on_abnormal_disconnect(broker):
    watcher = _client(broker.port, "watcher")
    watcher.subscribe(("status/#", 0))
    doomed = _client(
        broker.port, "doomed",
        will=("status/doomed", b"offline", 0, False),
    )
    doomed.close()  # abnormal: TCP drop without DISCONNECT
    topic, payload, *_ = watcher.read_publish()
    assert (topic, payload) == ("status/doomed", b"offline")
    watcher.disconnect()


def test_will_not_published_on_graceful_disconnect(broker):
    watcher = _client(broker.port, "watcher")
    watcher.subscribe(("status/#", 0))
    polite = _client(
        broker.port, "polite",
        will=("status/polite", b"offline", 0, False),
    )
    polite.disconnect()  # proper DISCONNECT packet
    assert watcher.try_read_packet(timeout=0.3) is None
    watcher.disconnect()


def test_will_retain_stores_message(broker):
    _client(
        broker.port, "willret",
        will=("status/willret", b"gone", 1, True),
    ).close()
    sub = _client(broker.port, "sub")
    sub.subscribe(("status/willret", 0))
    topic, payload, _, retain, *_ = sub.read_publish()
    assert (topic, payload, retain) == ("status/willret", b"gone", True)
    sub.disconnect()
