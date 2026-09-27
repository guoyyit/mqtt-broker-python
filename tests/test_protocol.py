"""Protocol-level behaviours: ping, keepalive timeout, malformed packets."""

from __future__ import annotations

from tests.mqtt_client import MQTTTestClient


def _client(port, cid, **kw):
    c = MQTTTestClient("127.0.0.1", port)
    c.connect(cid, username="user", password="pass", **kw)
    return c


def test_pingreq_gets_pingresp(broker):
    c = _client(broker.port, "pinger")
    c.ping()
    c.disconnect()


def test_keepalive_timeout_drops_connection_and_fires_will(broker):
    watcher = _client(broker.port, "watcher")
    watcher.subscribe(("gone/#", 0))
    # keep_alive=1s: broker must drop the connection after ~1.5s of silence
    silent = _client(
        broker.port, "silent", keep_alive=1,
        will=("gone/silent", b"timeout", 0, False),
    )
    topic, payload, *_ = watcher.read_publish(timeout=5)
    assert (topic, payload) == ("gone/silent", b"timeout")
    silent.close()
    watcher.disconnect()


def test_first_packet_must_be_connect(broker):
    c = MQTTTestClient("127.0.0.1", broker.port)
    c.send(b"\xc0\x00")  # PINGREQ before CONNECT
    try:
        c.read_packet(timeout=1.0)
        raise AssertionError("server kept a pre-CONNECT PINGREQ connection")
    except (ConnectionError, TimeoutError):
        pass
    c.close()
