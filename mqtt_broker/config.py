"""Broker configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(slots=True)
class BrokerConfig:
    """Runtime configuration for the MQTT broker."""

    host: str = "127.0.0.1"
    port: int = 1883
    #: username -> password; empty dict means anonymous access allowed
    users: dict[str, str] = field(default_factory=dict)
    allow_anonymous: bool = False
    #: file used to persist sessions / retained messages / QoS state
    db_path: Path = Path("broker_state.db")
    #: append-only audit log file; None disables file audit
    audit_log_path: Path | None = Path("broker_audit.log")
    #: max queued messages kept for one offline persistent session
    max_queued_messages: int = 1000
