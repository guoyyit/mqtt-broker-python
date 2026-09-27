"""Per-client session state."""

from __future__ import annotations

from dataclasses import dataclass, field

from .storage import StoredMessage


@dataclass(slots=True)
class WillMessage:
    topic: str
    payload: bytes
    qos: int
    retain: bool


@dataclass
class Session:
    """Holds subscriptions and in-flight outbound state for one client id."""

    client_id: str
    clean_session: bool = True
    subscriptions: dict[str, int] = field(default_factory=dict)  # filter -> qos
    #: packet_id -> (message, qos, dup) awaiting PUBACK / PUBREC from this client
    inflight_out: dict[int, tuple[StoredMessage, int, bool]] = field(default_factory=dict)
    #: outbound QoS2 packet ids for which PUBREL has been sent (awaiting PUBCOMP)
    qos2_pubrel_sent: set[int] = field(default_factory=set)
    next_packet_id: int = 1

    def allocate_packet_id(self) -> int:
        """Return the next free outbound packet id (1..65535, never 0)."""
        for _ in range(65535):
            pid = self.next_packet_id
            self.next_packet_id = self.next_packet_id % 65535 + 1
            if pid not in self.inflight_out:
                return pid
        raise RuntimeError("no packet ids available")
