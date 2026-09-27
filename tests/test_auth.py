"""Requirement 1: username/password auth, rejection, audit events."""

from __future__ import annotations

import json

from tests.mqtt_client import MQTTTestClient


def test_valid_credentials_accepted(broker):
    c = MQTTTestClient("127.0.0.1", broker.port)
    sp, rc = c.connect("c1", username="user", password="pass")
    assert rc == 0 and sp == 0
    c.disconnect()


def test_wrong_password_rejected(broker):
    c = MQTTTestClient("127.0.0.1", broker.port)
    _, rc = c.connect("c2", username="user", password="nope")
    assert rc == 4  # bad username or password
    # connection must be closed right after the negative CONNACK
    try:
        c.read_packet(timeout=1.0)
        raise AssertionError("server kept a rejected connection open")
    except (ConnectionError, TimeoutError):
        pass
    c.close()


def test_unknown_user_rejected(broker):
    c = MQTTTestClient("127.0.0.1", broker.port)
    _, rc = c.connect("c3", username="ghost", password="pass")
    assert rc == 4
    c.close()


def test_missing_credentials_rejected_when_anonymous_disabled(broker):
    c = MQTTTestClient("127.0.0.1", broker.port)
    _, rc = c.connect("c4")
    assert rc == 4
    c.close()


def test_anonymous_allowed_when_configured(broker_factory):
    fx = broker_factory(users={}, allow_anonymous=True)
    c = MQTTTestClient("127.0.0.1", fx.port)
    _, rc = c.connect("anon")
    assert rc == 0
    c.disconnect()


def test_auth_events_audited(broker):
    ok = MQTTTestClient("127.0.0.1", broker.port)
    ok.connect("good", username="user", password="pass")
    bad = MQTTTestClient("127.0.0.1", broker.port)
    bad.connect("bad", username="user", password="wrong")
    ok.disconnect()
    bad.close()

    events = broker.broker.audit.events
    ok_events = [e for e in events if e["event"] == "connect_ok"]
    rej_events = [e for e in events if e["event"] == "connect_rejected"]
    assert any(e["client_id"] == "good" for e in ok_events)
    assert any(e["client_id"] == "bad" and e["reason"] == "bad_credentials"
               for e in rej_events)

    # audit records are also persisted as JSON lines
    lines = broker.config.audit_log_path.read_text().strip().splitlines()
    persisted = [json.loads(line) for line in lines]
    assert any(e["event"] == "connect_ok" and e["client_id"] == "good"
               for e in persisted)
    assert any(e["event"] == "connect_rejected" and e["client_id"] == "bad"
               for e in persisted)
