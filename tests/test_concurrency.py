"""Requirement 6: concurrency consistency and ordering boundaries."""

from __future__ import annotations

import threading

from tests.mqtt_client import MQTTTestClient


def _client(port, cid, clean=True, **kw):
    c = MQTTTestClient("127.0.0.1", port)
    c.connect(cid, clean=clean, username="user", password="pass", **kw)
    return c


def test_in_order_delivery_per_connection(broker):
    sub = _client(broker.port, "sub")
    pub = _client(broker.port, "pub")
    sub.subscribe(("ord/#", 1))
    n = 50
    for i in range(n):
        pub.publish("ord/t", f"m{i:03d}".encode(), qos=1)
    for _ in range(n):
        pub.expect_packet(4)
    for i in range(n):
        _, payload, _, _, _, pid = sub.read_publish()
        assert payload == f"m{i:03d}".encode(), f"out of order at {i}"
        sub.puback(pid)
    sub.disconnect()
    pub.disconnect()


def test_concurrent_publishers_no_loss_no_interleave(broker):
    sub = _client(broker.port, "sub")
    sub.subscribe(("conc/#", 1))
    n_pub, n_msg = 5, 20

    def publish_many(idx: int) -> None:
        c = _client(broker.port, f"pub{idx}")
        for i in range(n_msg):
            c.publish("conc/t", f"p{idx}-{i:03d}".encode(), qos=1)
        for _ in range(n_msg):
            c.expect_packet(4)
        c.disconnect()

    threads = [threading.Thread(target=publish_many, args=(i,)) for i in range(n_pub)]
    for t in threads:
        t.start()

    received: dict[int, list[int]] = {i: [] for i in range(n_pub)}
    for _ in range(n_pub * n_msg):
        _, payload, _, _, _, pid = sub.read_publish(timeout=10)
        sub.puback(pid)
        p_idx, seq = payload.decode()[1:].split("-")
        received[int(p_idx)].append(int(seq))
    for t in threads:
        t.join()

    # every message arrived exactly once, and each publisher's stream
    # kept its own order (cross-publisher interleaving is allowed)
    for i in range(n_pub):
        assert received[i] == list(range(n_msg)), f"publisher {i} stream broken"
    sub.disconnect()


def test_concurrent_subscribe_and_retained_updates(broker):
    """Hammer subscribe/unsubscribe, retained set/clear and publishing
    concurrently; the broker must stay consistent and responsive."""
    stop = threading.Event()
    errors: list[BaseException] = []

    def retained_writer() -> None:
        try:
            c = _client(broker.port, "rw")
            for i in range(30):
                c.publish("mix/ret", f"v{i}".encode(), qos=1, retain=True)
                c.expect_packet(4)
            c.publish("mix/ret", b"", retain=True)  # clear
            c.disconnect()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def resubscriber() -> None:
        try:
            c = _client(broker.port, "rs")
            for _ in range(30):
                c.subscribe(("mix/#", 1))
                c.unsubscribe("mix/#")
            c.disconnect()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def publisher() -> None:
        try:
            c = _client(broker.port, "pp")
            for i in range(30):
                c.publish("mix/live", f"x{i}".encode(), qos=1)
                c.expect_packet(4)
            c.disconnect()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=f) for f in
               (retained_writer, resubscriber, publisher)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive(), "broker stopped responding"
    assert not errors, errors

    # after the dust settles the retained value was cleared
    probe = _client(broker.port, "probe")
    probe.subscribe(("mix/ret", 0))
    assert probe.try_read_packet(timeout=0.3) is None
    probe.disconnect()


def test_takeover_disconnects_stale_connection(broker):
    first = _client(broker.port, "same-id", clean=False)
    first.subscribe(("to/#", 1))
    second = _client(broker.port, "same-id", clean=False)  # same client id
    # the stale connection must have been closed by the broker
    try:
        first.read_packet(timeout=1.0)
        raise AssertionError("stale connection still open")
    except (ConnectionError, TimeoutError):
        pass
    pub = _client(broker.port, "pub")
    pub.publish("to/x", b"once", qos=1)
    pub.expect_packet(4)
    # only the live connection receives the message
    _, payload, _, _, _, pid = second.read_publish()
    assert payload == b"once"
    second.puback(pid)
    second.disconnect()
    pub.disconnect()
    first.close()
