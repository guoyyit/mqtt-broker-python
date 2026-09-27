"""Requirement 7: crash recovery — durable state survives a broker restart,
incomplete transactions are never treated as acknowledged."""

from __future__ import annotations

from tests.mqtt_client import MQTTTestClient


def _client(port, cid, clean=True, **kw):
    c = MQTTTestClient("127.0.0.1", port)
    c.connect(cid, clean=clean, username="user", password="pass", **kw)
    return c


def _restart(broker_factory, fx, db_path):
    """Stop the broker and start a fresh one on the same database file."""
    fx.stop()
    return broker_factory(db_path=db_path)


def test_retained_survives_restart(broker_factory, tmp_path):
    db = tmp_path / "r.db"
    fx = broker_factory(db_path=db)
    pub = _client(fx.port, "pub")
    pub.publish("keep/me", b"R", qos=1, retain=True)
    pub.expect_packet(4)
    pub.disconnect()

    fx2 = _restart(broker_factory, fx, db)
    sub = _client(fx2.port, "sub")
    sub.subscribe(("keep/#", 0))
    topic, payload, _, retain, *_ = sub.read_publish()
    assert (topic, payload, retain) == ("keep/me", b"R", True)
    sub.disconnect()


def test_persistent_session_and_queue_survive_restart(broker_factory, tmp_path):
    db = tmp_path / "q.db"
    fx = broker_factory(db_path=db)
    sub = _client(fx.port, "pers", clean=False)
    sub.subscribe(("s/#", 1))
    sub.disconnect()
    pub = _client(fx.port, "pub")
    pub.publish("s/1", b"queued", qos=1)
    pub.expect_packet(4)
    pub.disconnect()

    fx2 = _restart(broker_factory, fx, db)
    back = MQTTTestClient("127.0.0.1", fx2.port)
    sp, rc = back.connect("pers", clean=False, username="user", password="pass")
    assert rc == 0 and sp == 1  # session restored
    _, payload, qos, _, _, pid = back.read_publish()
    assert payload == b"queued" and qos == 1
    back.puback(pid)
    back.disconnect()


def test_acknowledged_messages_not_redelivered_after_restart(broker_factory, tmp_path):
    db = tmp_path / "a.db"
    fx = broker_factory(db_path=db)
    sub = _client(fx.port, "acker", clean=False)
    sub.subscribe(("a/#", 1))
    pub = _client(fx.port, "pub")
    pub.publish("a/1", b"done", qos=1)
    pub.expect_packet(4)
    _, _, _, _, _, pid = sub.read_publish()
    sub.puback(pid)  # fully acknowledged before restart
    sub.disconnect()
    pub.disconnect()

    fx2 = _restart(broker_factory, fx, db)
    back = _client(fx2.port, "acker", clean=False)
    assert back.try_read_packet(timeout=0.3) is None
    back.disconnect()


def test_unacknowledged_qos1_redelivered_after_restart(broker_factory, tmp_path):
    db = tmp_path / "u.db"
    fx = broker_factory(db_path=db)
    sub = _client(fx.port, "dropper", clean=False)
    sub.subscribe(("u/#", 1))
    pub = _client(fx.port, "pub")
    pub.publish("u/1", b"pending", qos=1)
    pub.expect_packet(4)
    sub.read_publish()  # received but never PUBACKed
    sub.disconnect()
    pub.disconnect()

    fx2 = _restart(broker_factory, fx, db)
    back = _client(fx2.port, "dropper", clean=False)
    _, payload, _, _, dup, pid = back.read_publish()
    assert payload == b"pending" and dup  # DUP set on recovery redelivery
    back.puback(pid)
    back.disconnect()


def test_inbound_qos2_transaction_survives_restart(broker_factory, tmp_path):
    db = tmp_path / "t.db"
    fx = broker_factory(db_path=db)
    sub = _client(fx.port, "sub")
    sub.subscribe(("t2", 2))
    pub = _client(fx.port, "pub", clean=False)
    pid = pub.publish("t2", b"tx", qos=2)
    pub.expect_packet(5)  # PUBREC — broker has durably stored the transaction
    pub.close()
    sub.disconnect()

    # restart before PUBREL: the half-finished transaction must not have
    # delivered anything, and must still complete afterwards
    fx2 = _restart(broker_factory, fx, db)
    sub2 = _client(fx2.port, "sub")
    sub2.subscribe(("t2", 2))
    assert sub2.try_read_packet(timeout=0.3) is None  # nothing delivered early
    pub2 = _client(fx2.port, "pub", clean=False)
    pub2.pubrel(pid)
    pub2.expect_packet(7)  # PUBCOMP
    assert sub2.read_publish()[1] == b"tx"  # delivered exactly once, now
    sub2.disconnect()
    pub2.disconnect()
