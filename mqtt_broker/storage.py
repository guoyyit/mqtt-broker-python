"""SQLite-backed durable storage for sessions, retained messages and QoS state.

Durability contract: every mutating method commits before returning, so an
acknowledgement sent after the call never refers to state that a crash could
lose.  WAL + synchronous=FULL gives atomic commits; a crash mid-transaction
simply rolls back, so incomplete transactions are never marked acknowledged.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class StoredMessage:
    topic: str
    payload: bytes
    qos: int
    retain: bool


@dataclass(slots=True)
class StoredSubscription:
    topic_filter: str
    qos: int


_SCHEMA = """
CREATE TABLE IF NOT EXISTS retained (
    topic   TEXT PRIMARY KEY,
    payload BLOB NOT NULL,
    qos     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
    client_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS subscriptions (
    client_id    TEXT NOT NULL,
    topic_filter TEXT NOT NULL,
    qos          INTEGER NOT NULL,
    PRIMARY KEY (client_id, topic_filter)
);
CREATE TABLE IF NOT EXISTS queue (
    client_id TEXT NOT NULL,
    packet_id INTEGER NOT NULL,
    topic     TEXT NOT NULL,
    payload   BLOB NOT NULL,
    qos       INTEGER NOT NULL,
    retain    INTEGER NOT NULL,
    PRIMARY KEY (client_id, packet_id)
);
CREATE TABLE IF NOT EXISTS inflight_qos2 (
    client_id TEXT NOT NULL,
    packet_id INTEGER NOT NULL,
    topic     TEXT NOT NULL,
    payload   BLOB NOT NULL,
    qos       INTEGER NOT NULL,
    retain    INTEGER NOT NULL,
    PRIMARY KEY (client_id, packet_id)
);
"""


class Storage:
    """Durable state. All writes are committed before the broker acknowledges."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        with self._lock, self._db:
            self._db.executescript(_SCHEMA)

    # --- retained messages ---
    def set_retained(self, topic: str, payload: bytes, qos: int) -> None:
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO retained(topic, payload, qos) VALUES (?,?,?)",
                (topic, payload, qos),
            )

    def clear_retained(self, topic: str) -> None:
        with self._lock, self._db:
            self._db.execute("DELETE FROM retained WHERE topic = ?", (topic,))

    def get_retained(self) -> list[StoredMessage]:
        with self._lock:
            rows = self._db.execute(
                "SELECT topic, payload, qos FROM retained ORDER BY rowid"
            ).fetchall()
        return [StoredMessage(t, bytes(p), q, True) for t, p, q in rows]

    # --- persistent sessions ---
    def save_session(self, client_id: str) -> None:
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO sessions(client_id) VALUES (?)", (client_id,)
            )

    def delete_session(self, client_id: str) -> None:
        with self._lock, self._db:
            self._db.execute("DELETE FROM sessions WHERE client_id = ?", (client_id,))
            self._db.execute("DELETE FROM subscriptions WHERE client_id = ?", (client_id,))
            self._db.execute("DELETE FROM queue WHERE client_id = ?", (client_id,))
            self._db.execute("DELETE FROM inflight_qos2 WHERE client_id = ?", (client_id,))

    def list_sessions(self) -> list[str]:
        with self._lock:
            rows = self._db.execute("SELECT client_id FROM sessions ORDER BY rowid").fetchall()
        return [r[0] for r in rows]

    def session_exists(self, client_id: str) -> bool:
        with self._lock:
            row = self._db.execute(
                "SELECT 1 FROM sessions WHERE client_id = ?", (client_id,)
            ).fetchone()
        return row is not None

    def save_subscription(self, client_id: str, topic_filter: str, qos: int) -> None:
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO subscriptions(client_id, topic_filter, qos)"
                " VALUES (?,?,?)",
                (client_id, topic_filter, qos),
            )

    def remove_subscription(self, client_id: str, topic_filter: str) -> None:
        with self._lock, self._db:
            self._db.execute(
                "DELETE FROM subscriptions WHERE client_id = ? AND topic_filter = ?",
                (client_id, topic_filter),
            )

    def get_subscriptions(self, client_id: str) -> list[StoredSubscription]:
        with self._lock:
            rows = self._db.execute(
                "SELECT topic_filter, qos FROM subscriptions WHERE client_id = ?"
                " ORDER BY rowid",
                (client_id,),
            ).fetchall()
        return [StoredSubscription(f, q) for f, q in rows]

    # --- offline queue ---
    def enqueue(self, client_id: str, msg: StoredMessage, packet_id: int) -> None:
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO queue(client_id, packet_id, topic, payload, qos, retain)"
                " VALUES (?,?,?,?,?,?)",
                (client_id, packet_id, msg.topic, msg.payload, msg.qos, int(msg.retain)),
            )

    def dequeue(self, client_id: str, packet_id: int) -> None:
        with self._lock, self._db:
            self._db.execute(
                "DELETE FROM queue WHERE client_id = ? AND packet_id = ?",
                (client_id, packet_id),
            )

    def get_queued(self, client_id: str) -> list[tuple[int, StoredMessage]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT packet_id, topic, payload, qos, retain FROM queue"
                " WHERE client_id = ? ORDER BY packet_id",
                (client_id,),
            ).fetchall()
        return [
            (pid, StoredMessage(t, bytes(p), q, bool(r))) for pid, t, p, q, r in rows
        ]

    # --- QoS 2 inbound transaction state ---
    def save_inflight_qos2(self, client_id: str, packet_id: int, msg: StoredMessage) -> None:
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO inflight_qos2(client_id, packet_id, topic, payload, qos, retain)"
                " VALUES (?,?,?,?,?,?)",
                (client_id, packet_id, msg.topic, msg.payload, msg.qos, int(msg.retain)),
            )

    def get_inflight_qos2(self, client_id: str, packet_id: int) -> StoredMessage | None:
        with self._lock:
            row = self._db.execute(
                "SELECT topic, payload, qos, retain FROM inflight_qos2"
                " WHERE client_id = ? AND packet_id = ?",
                (client_id, packet_id),
            ).fetchone()
        if row is None:
            return None
        t, p, q, r = row
        return StoredMessage(t, bytes(p), q, bool(r))

    def delete_inflight_qos2(self, client_id: str, packet_id: int) -> None:
        with self._lock, self._db:
            self._db.execute(
                "DELETE FROM inflight_qos2 WHERE client_id = ? AND packet_id = ?",
                (client_id, packet_id),
            )

    def close(self) -> None:
        with self._lock:
            self._db.close()
