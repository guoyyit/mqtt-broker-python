"""Core broker: sessions, subscriptions, retained messages, QoS flows.

Concurrency model: the broker runs on a single asyncio loop.  All state
mutations happen under ``self._lock``; outbound bytes are pushed into
per-connection queues (non-blocking ``put_nowait``), so one connection's
slow consumer can never block another.  Ordering guarantee: bytes pushed
to the same connection are delivered in push order; no ordering is
promised across different connections beyond causality.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import dataclass
from typing import Callable

from .audit import AuditLogger
from .auth import Authenticator
from .config import BrokerConfig
from .packet import (
    ConnectPacket,
    ConnectReturnCode,
    PublishPacket,
    SubAckReason,
    SubscribePacket,
    UnsubscribePacket,
    encode_connack,
    encode_puback,
    encode_pubcomp,
    encode_publish,
    encode_pubrec,
    encode_pubrel,
    encode_suback,
    encode_unsuback,
)
from .session import Session, WillMessage
from .storage import Storage, StoredMessage
from .topics import topic_matches, validate_topic_filter, validate_topic_name

log = logging.getLogger(__name__)

#: pushes raw bytes into one connection's outbound queue; never blocks
SendFn = Callable[[bytes], None]
#: asks the transport to close the connection identified by a token
CloseFn = Callable[["Connection"], None]


@dataclass(slots=True)
class Connection:
    """Handle for one online client; used as an opaque token by the server."""

    client_id: str
    send: SendFn
    session: Session
    token: int = 0


class Broker:
    """Transport-independent broker core; the TCP server drives this."""

    def __init__(self, config: BrokerConfig) -> None:
        self.config = config
        self.storage = Storage(config.db_path)
        self.audit = AuditLogger(config.audit_log_path)
        self.authenticator = Authenticator(config)
        self._lock = asyncio.Lock()
        self._sessions: dict[str, Session] = {}
        self._connections: dict[int, Connection] = {}  # token id -> connection
        self._conn_by_client: dict[str, Connection] = {}
        self._wills: dict[str, WillMessage] = {}
        self._token_counter = itertools.count(1)
        #: set by the server so the broker can force-close a taken-over connection
        self.connection_closer: CloseFn | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Recover persistent sessions from durable storage (crash recovery)."""
        async with self._lock:
            for client_id in self.storage.list_sessions():
                session = Session(client_id=client_id, clean_session=False)
                for sub in self.storage.get_subscriptions(client_id):
                    session.subscriptions[sub.topic_filter] = sub.qos
                queued = self.storage.get_queued(client_id)
                for pid, msg in queued:
                    # re-deliver with DUP set: previous attempt was never acked
                    session.inflight_out[pid] = (msg, msg.qos, True)
                if queued:
                    session.next_packet_id = max(pid for pid, _ in queued) + 1
                self._sessions[client_id] = session
        self.audit.log("broker_started", recovered_sessions=len(self._sessions))

    async def stop(self) -> None:
        self.audit.log("broker_stopped")

    def close(self) -> None:
        self.storage.close()
        self.audit.close()

    # ------------------------------------------------------------------
    # connection handling
    # ------------------------------------------------------------------

    async def connect(self, send: SendFn, pkt: ConnectPacket) -> Connection | None:
        """Authenticate and register a connection; returns token or None.

        The CONNACK is pushed through *send*; on rejection the server must
        close the transport after flushing.
        """
        async with self._lock:
            client_id = pkt.client_id
            if not client_id:
                if not pkt.clean_session:
                    self.audit.log("connect_rejected", client_id="", reason="empty_client_id")
                    send(encode_connack(False, ConnectReturnCode.IDENTIFIER_REJECTED))
                    return None
                client_id = f"auto-{next(self._token_counter):08x}"

            if not self.authenticator.authenticate(pkt.username, pkt.password):
                self.audit.log(
                    "connect_rejected",
                    client_id=client_id,
                    username=pkt.username,
                    reason="bad_credentials",
                )
                send(encode_connack(False, ConnectReturnCode.BAD_USERNAME_OR_PASSWORD))
                return None

            session_present = False
            if pkt.clean_session:
                # any previous durable state for this id is discarded
                self.storage.delete_session(client_id)
                self._sessions.pop(client_id, None)
                session = Session(client_id=client_id, clean_session=True)
            else:
                session = self._sessions.get(client_id)
                if session is None:
                    session = Session(client_id=client_id, clean_session=False)
                    self.storage.save_session(client_id)
                else:
                    session_present = True
                # restore unacknowledged outbound messages for re-delivery
                for pid, msg in self.storage.get_queued(client_id):
                    if pid not in session.inflight_out:
                        session.inflight_out[pid] = (msg, msg.qos, True)
                # everything still unacknowledged at reconnect is a
                # retransmission, so it must go out with DUP set
                for pid, (msg, qos, _dup) in session.inflight_out.items():
                    session.inflight_out[pid] = (msg, qos, True)
                if session.inflight_out:
                    session.next_packet_id = max(
                        max(session.inflight_out), session.next_packet_id
                    ) + 1

            old = self._conn_by_client.get(client_id)
            if old is not None:
                # session takeover: the stale connection is dropped; its will
                # (if any) is published because this is an abnormal disconnect
                self._publish_will(old.client_id)
                if self.connection_closer is not None:
                    self.connection_closer(old)
                self.audit.log("takeover", client_id=client_id)

            conn = Connection(client_id=client_id, send=send, session=session)
            conn.token = next(self._token_counter)
            self._connections[conn.token] = conn
            self._conn_by_client[client_id] = conn
            self._sessions[client_id] = session

            if pkt.will_topic is not None and pkt.will_payload is not None:
                self._wills[client_id] = WillMessage(
                    topic=pkt.will_topic,
                    payload=pkt.will_payload,
                    qos=pkt.will_qos,
                    retain=pkt.will_retain,
                )
            else:
                self._wills.pop(client_id, None)

            self.audit.log(
                "connect_ok",
                client_id=client_id,
                username=pkt.username,
                clean_session=pkt.clean_session,
                session_present=session_present,
            )
            send(encode_connack(session_present, ConnectReturnCode.ACCEPTED))
            return conn

    async def session_established(self, conn: Connection) -> None:
        """Re-deliver unacknowledged outbound messages after CONNACK."""
        async with self._lock:
            if self._conn_by_client.get(conn.client_id) is not conn:
                return
            for pid, (msg, qos, dup) in sorted(conn.session.inflight_out.items()):
                conn.send(
                    encode_publish(
                        PublishPacket(
                            topic=msg.topic,
                            payload=msg.payload,
                            qos=qos,
                            retain=msg.retain,
                            dup=dup,
                            packet_id=pid if qos else None,
                        )
                    )
                )

    async def disconnect(self, conn: Connection, graceful: bool) -> None:
        """Tear down a connection; publishes the will unless *graceful*."""
        async with self._lock:
            self._connections.pop(conn.token, None)
            if self._conn_by_client.get(conn.client_id) is not conn:
                return  # this connection was already taken over
            del self._conn_by_client[conn.client_id]

            if graceful:
                self._wills.pop(conn.client_id, None)
            else:
                self._publish_will(conn.client_id)

            session = self._sessions.get(conn.client_id)
            if session is not None and session.clean_session:
                self._sessions.pop(conn.client_id, None)
                self.storage.delete_session(conn.client_id)
            self.audit.log(
                "disconnect", client_id=conn.client_id, graceful=graceful
            )

    def _publish_will(self, client_id: str) -> None:
        will = self._wills.pop(client_id, None)
        if will is None:
            return
        self.audit.log("will_published", client_id=client_id, topic=will.topic)
        msg = StoredMessage(will.topic, will.payload, will.qos, will.retain)
        self._apply_retain(msg)
        self._route(msg)

    # ------------------------------------------------------------------
    # publish path
    # ------------------------------------------------------------------

    async def publish(self, conn: Connection, pkt: PublishPacket) -> None:
        """Handle an inbound PUBLISH from a client."""
        async with self._lock:
            if self._conn_by_client.get(conn.client_id) is not conn:
                return
            if not validate_topic_name(pkt.topic):
                return  # protocol violation; server drops the connection
            msg = StoredMessage(pkt.topic, pkt.payload, pkt.qos, pkt.retain)
            if pkt.qos == 0:
                self._apply_retain(msg)
                self._route(msg)
            elif pkt.qos == 1:
                self._apply_retain(msg)
                self._route(msg)
                assert pkt.packet_id is not None
                conn.send(encode_puback(pkt.packet_id))
            else:  # qos 2: store first, deliver only on PUBREL
                assert pkt.packet_id is not None
                if self.storage.get_inflight_qos2(conn.client_id, pkt.packet_id) is None:
                    self.storage.save_inflight_qos2(conn.client_id, pkt.packet_id, msg)
                conn.send(encode_pubrec(pkt.packet_id))

    async def pubrel_received(self, conn: Connection, packet_id: int) -> None:
        """Second half of inbound QoS 2: deliver exactly once, then PUBCOMP."""
        async with self._lock:
            if self._conn_by_client.get(conn.client_id) is not conn:
                return
            msg = self.storage.get_inflight_qos2(conn.client_id, packet_id)
            if msg is not None:
                self.storage.delete_inflight_qos2(conn.client_id, packet_id)
                self._apply_retain(msg)
                self._route(msg)
            conn.send(encode_pubcomp(packet_id))

    # ------------------------------------------------------------------
    # outbound acknowledgements from clients
    # ------------------------------------------------------------------

    async def puback_received(self, conn: Connection, packet_id: int) -> None:
        async with self._lock:
            session = conn.session
            if session.inflight_out.pop(packet_id, None) is not None:
                self._ack_persisted(session, packet_id)

    async def pubrec_received(self, conn: Connection, packet_id: int) -> None:
        async with self._lock:
            if packet_id in conn.session.inflight_out:
                conn.session.qos2_pubrel_sent.add(packet_id)
                conn.send(encode_pubrel(packet_id))

    async def pubcomp_received(self, conn: Connection, packet_id: int) -> None:
        async with self._lock:
            session = conn.session
            session.qos2_pubrel_sent.discard(packet_id)
            if session.inflight_out.pop(packet_id, None) is not None:
                self._ack_persisted(session, packet_id)

    def _ack_persisted(self, session: Session, packet_id: int) -> None:
        """An outbound message was acknowledged: remove its durable copy."""
        if not session.clean_session:
            self.storage.dequeue(session.client_id, packet_id)

    # ------------------------------------------------------------------
    # subscribe / unsubscribe
    # ------------------------------------------------------------------

    async def subscribe(self, conn: Connection, pkt: SubscribePacket) -> None:
        async with self._lock:
            if self._conn_by_client.get(conn.client_id) is not conn:
                return
            session = conn.session
            reasons: list[SubAckReason] = []
            accepted: list[tuple[str, int]] = []
            for topic_filter, qos in pkt.subscriptions:
                if not validate_topic_filter(topic_filter):
                    reasons.append(SubAckReason.FAILURE)
                    continue
                session.subscriptions[topic_filter] = qos
                if not session.clean_session:
                    self.storage.save_subscription(
                        session.client_id, topic_filter, qos
                    )
                reasons.append(SubAckReason(qos))
                accepted.append((topic_filter, qos))
            conn.send(encode_suback(pkt.packet_id, reasons))
            # retained messages are delivered right after the SUBACK
            for topic_filter, sub_qos in accepted:
                for msg in self.storage.get_retained():
                    if topic_matches(topic_filter, msg.topic):
                        self._deliver(
                            conn, msg, min(msg.qos, sub_qos), retain=True
                        )

    async def unsubscribe(self, conn: Connection, pkt: UnsubscribePacket) -> None:
        async with self._lock:
            if self._conn_by_client.get(conn.client_id) is not conn:
                return
            session = conn.session
            for topic_filter in pkt.filters:
                if session.subscriptions.pop(topic_filter, None) is not None:
                    if not session.clean_session:
                        self.storage.remove_subscription(
                            session.client_id, topic_filter
                        )
            conn.send(encode_unsuback(pkt.packet_id))

    # ------------------------------------------------------------------
    # routing internals (must be called with the lock held)
    # ------------------------------------------------------------------

    def _apply_retain(self, msg: StoredMessage) -> None:
        if not msg.retain:
            return
        if msg.payload:
            self.storage.set_retained(msg.topic, msg.payload, msg.qos)
        else:
            # empty retained payload clears the retained message
            self.storage.clear_retained(msg.topic)

    def _route(self, msg: StoredMessage) -> None:
        """Deliver to every matching session; queue for offline persistent ones."""
        for client_id, session in list(self._sessions.items()):
            grant: int | None = None
            for topic_filter, sub_qos in session.subscriptions.items():
                if topic_matches(topic_filter, msg.topic):
                    grant = sub_qos if grant is None else max(grant, sub_qos)
            if grant is None:
                continue
            effective_qos = min(msg.qos, grant)
            conn = self._conn_by_client.get(client_id)
            if conn is not None:
                self._deliver(conn, msg, effective_qos, retain=msg.retain)
            elif not session.clean_session and effective_qos > 0:
                # offline persistent session: queue durably for reconnect
                pid = session.allocate_packet_id()
                stored = StoredMessage(msg.topic, msg.payload, effective_qos, False)
                self.storage.enqueue(client_id, stored, pid)
                session.inflight_out[pid] = (stored, effective_qos, False)

    def _deliver(self, conn: Connection, msg: StoredMessage, qos: int, retain: bool) -> None:
        session = conn.session
        packet_id: int | None = None
        if qos > 0:
            packet_id = session.allocate_packet_id()
            stored = StoredMessage(msg.topic, msg.payload, qos, retain)
            session.inflight_out[packet_id] = (stored, qos, False)
            if not session.clean_session:
                # persist before sending: an ack must never refer to state
                # that a crash could lose
                self.storage.enqueue(session.client_id, stored, packet_id)
        conn.send(
            encode_publish(
                PublishPacket(
                    topic=msg.topic,
                    payload=msg.payload,
                    qos=qos,
                    retain=retain,
                    dup=False,
                    packet_id=packet_id,
                )
            )
        )
