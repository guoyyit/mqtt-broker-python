"""The MQTT broker runs inside the FastAPI service process (lifespan)."""

from __future__ import annotations

import importlib

import pytest
from fastapi.testclient import TestClient

from tests.mqtt_client import MQTTTestClient


@pytest.fixture()
def service(monkeypatch, tmp_path):
    monkeypatch.setenv("MQTT_PORT", "0")  # ephemeral loopback port
    monkeypatch.setenv("MQTT_USERS", "svc:pw")
    monkeypatch.setenv("MQTT_DB_PATH", str(tmp_path / "svc.db"))
    monkeypatch.setenv("MQTT_AUDIT_LOG", str(tmp_path / "svc_audit.log"))
    import main
    importlib.reload(main)
    with TestClient(main.app) as client:
        yield client


def test_http_root_still_works(service):
    assert service.get("/").json() == {"Hello": "World"}


def test_mqtt_broker_accepts_connections_in_process(service):
    status = service.get("/mqtt/status").json()
    assert status["host"] == "127.0.0.1" and status["port"] > 0

    sub = MQTTTestClient("127.0.0.1", status["port"])
    _, rc = sub.connect("sub", username="svc", password="pw")
    assert rc == 0
    sub.subscribe(("svc/#", 0))

    pub = MQTTTestClient("127.0.0.1", status["port"])
    pub.connect("pub", username="svc", password="pw")
    pub.publish("svc/hello", b"via-fastapi-process")
    assert sub.read_publish()[1] == b"via-fastapi-process"

    # wrong credentials are rejected by the in-process broker
    bad = MQTTTestClient("127.0.0.1", status["port"])
    _, rc = bad.connect("bad", username="svc", password="nope")
    assert rc == 4

    sub.disconnect()
    pub.disconnect()
    bad.close()
