"""接入鉴权与可审计事件。"""

from __future__ import annotations

import json

import pytest

from mqtt_broker.config import BrokerConfig
from mqtt_broker.server import MQTTServer
from mqtt_broker.testclient import MQTTTestClient


def _read_audit(path) -> list[dict]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


async def _start(tmp_path, *, credentials=None, audit=True):
    import socket
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    audit_path = str(tmp_path / "audit.log") if audit else None
    cfg = BrokerConfig(
        host="127.0.0.1", port=port,
        credentials=credentials if credentials is not None else {"user": "pass"},
        data_dir=str(tmp_path / "data"), audit_log=audit_path)
    server = MQTTServer(cfg)
    await server.start()
    return server, port, audit_path


async def test_wrong_password_rejected_immediately(tmp_path):
    server, port, audit_path = await _start(tmp_path)
    try:
        client = MQTTTestClient(port=port)
        present, accepted = await client.connect(
            "c", clean_session=True, username="user", password="bad")
        assert accepted is False and present is False
        # 未授权连接必须立即拒绝：服务端随即关闭 TCP（客户端读到 EOF）。
        assert await client.reader.read(1) == b""
    finally:
        await server.stop()
    events = _read_audit(audit_path)
    rejects = [e for e in events if e["outcome"] == "rejected"]
    assert len(rejects) == 1
    assert rejects[0]["reason"] == "invalid_credentials"
    assert rejects[0]["username"] == "user"


async def test_missing_credentials_rejected(tmp_path):
    server, port, audit_path = await _start(tmp_path)
    try:
        client = MQTTTestClient(port=port)
        _, accepted = await client.connect("c", clean_session=True)
        assert accepted is False
    finally:
        await server.stop()
    events = _read_audit(audit_path)
    assert any(e["outcome"] == "rejected"
               and e["reason"] == "missing_credentials" for e in events)


async def test_unknown_user_rejected(tmp_path):
    server, port, audit_path = await _start(tmp_path)
    try:
        client = MQTTTestClient(port=port)
        _, accepted = await client.connect(
            "c", clean_session=True, username="ghost", password="pass")
        assert accepted is False
    finally:
        await server.stop()
    events = _read_audit(audit_path)
    assert any(e["outcome"] == "rejected"
               and e["reason"] == "invalid_credentials" for e in events)


async def test_accepted_event_audited(tmp_path):
    server, port, audit_path = await _start(tmp_path)
    try:
        client = MQTTTestClient(port=port)
        _, accepted = await client.connect(
            "ok-client", clean_session=True, username="user", password="pass")
        assert accepted is True
        await client.disconnect()
        import asyncio
        await asyncio.sleep(0.1)
    finally:
        await server.stop()
    events = _read_audit(audit_path)
    accepted_events = [e for e in events if e["outcome"] == "accepted"]
    assert len(accepted_events) == 1
    assert accepted_events[0]["client_id"] == "ok-client"
    assert accepted_events[0]["username"] == "user"
    assert any(e["outcome"] == "graceful" for e in events)


async def test_anonymous_allowed_when_no_credentials(tmp_path):
    server, port, _ = await _start(tmp_path, credentials={})
    try:
        client = MQTTTestClient(port=port)
        _, accepted = await client.connect("anon", clean_session=True)
        assert accepted is True
        await client.disconnect()
    finally:
        await server.stop()
