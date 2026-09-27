"""Requirement 3: QoS 0/1/2 semantics, duplicates and reconnect boundaries."""

from __future__ import annotations

import struct

from tests.mqtt_client import MQTTTestClient


def _client(port, cid, clean=True, **kw):
    c = MQTTTestClient("127.0.0.1", port)
    c.connect(cid, clean=clean, username="user", password="pass", **kw)
    return c


def test_qos0_no_ack_and_delivery(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("q0", 0))
    pub.publish("q0", b"m0", qos=0)
    # publisher gets nothing back
    assert pub.try_read_packet(timeout=0.3) is None
    assert sub.read_publish()[1] == b"m0"
    sub.disconnect()
    pub.disconnect()


def test_qos1_puback_and_delivery(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("q1", 1))
    pid = pub.publish("q1", b"m1", qos=1)
    _, body = pub.expect_packet(4)  # PUBACK
    assert struct.unpack("!H", body)[0] == pid
    topic, payload, qos, _, _, spid = sub.read_publish()
    assert (topic, payload, qos) == ("q1", b"m1", 1)
    assert spid is not None
    sub.puback(spid)
    sub.disconnect()
    pub.disconnect()


def test_qos1_downgrade_to_subscription_qos(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("down", 0))  # subscribed at qos0
    pub.publish("down", b"d", qos=1)
    pub.expect_packet(4)
    _, _, qos, _, _, spid = sub.read_publish()
    assert qos == 0 and spid is None
    sub.disconnect()
    pub.disconnect()


def test_qos2_full_handshake(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("q2", 2))
    pid = pub.publish("q2", b"m2", qos=2)
    _, body = pub.expect_packet(5)  # PUBREC
    assert struct.unpack("!H", body)[0] == pid
    pub.pubrel(pid)
    pub.expect_packet(7)  # PUBCOMP
    topic, payload, qos, _, _, spid = sub.read_publish()
    assert (topic, payload, qos) == ("q2", b"m2", 2)
    # complete the outbound qos2 handshake as the subscriber
    sub.pubrec(spid)
    _, body = sub.expect_packet(6)  # PUBREL
    assert struct.unpack("!H", body)[0] == spid
    sub.pubcomp(spid)
    sub.disconnect()
    pub.disconnect()


def test_qos2_duplicate_publish_not_delivered_twice(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("q2dup", 2))
    pid = pub.publish("q2dup", b"once", qos=2)
    pub.expect_packet(5)  # PUBREC
    # publisher retransmits the same PUBLISH (DUP) before PUBREL
    pub.publish("q2dup", b"once", qos=2, dup=True, pid=pid)
    pub.expect_packet(5)  # PUBREC again
    pub.pubrel(pid)
    pub.expect_packet(7)  # PUBCOMP
    assert sub.read_publish()[1] == b"once"
    # exactly once: no second delivery
    assert sub.try_read_packet(timeout=0.3) is None
    sub.disconnect()
    pub.disconnect()


def test_qos1_redelivery_after_unacknowledged_disconnect(broker):
    # persistent subscriber receives a qos1 message but drops before PUBACK
    sub = _client(broker.port, "pers", clean=False)
    sub.subscribe(("re", 1))
    pub = _client(broker.port, "pub")
    pub.publish("re", b"again", qos=1)
    pub.expect_packet(4)
    _, payload, _, _, dup1, _ = sub.read_publish()
    assert payload == b"again" and not dup1
    sub.close()  # abnormal drop, no PUBACK sent

    sub2 = _client(broker.port, "pers", clean=False)
    _, payload, _, _, dup2, pid2 = sub2.read_publish()
    assert payload == b"again" and dup2  # redelivered with DUP set
    sub2.puback(pid2)
    # acknowledged now: a further reconnect must not redeliver
    sub2.disconnect()
    sub3 = _client(broker.port, "pers", clean=False)
    assert sub3.try_read_packet(timeout=0.3) is None
    sub3.disconnect()
    pub.disconnect()


def test_qos2_inbound_survives_publisher_reconnect(broker):
    # persistent publisher completes PUBLISH/PUBREC, drops before PUBREL,
    # then resumes the transaction after reconnecting
    sub = _client(broker.port, "sub")
    sub.subscribe(("q2r", 2))
    pub = _client(broker.port, "pub", clean=False)
    pid = pub.publish("q2r", b"tx", qos=2)
    pub.expect_packet(5)  # PUBREC
    pub.close()  # drop mid-transaction

    pub2 = _client(broker.port, "pub", clean=False)
    pub2.pubrel(pid)  # resume the transaction
    pub2.expect_packet(7)  # PUBCOMP
    assert sub.read_publish()[1] == b"tx"
    # no duplicate after the transaction completed
    assert sub.try_read_packet(timeout=0.3) is None
    sub.disconnect()
    pub2.disconnect()
