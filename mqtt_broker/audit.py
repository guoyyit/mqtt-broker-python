"""接入与安全事件的可审计日志。

每条审计事件输出为一行 JSON（JSON Lines），字段固定、便于检索：
``ts / event / outcome / client_id / username / remote / reason``。
写入通过锁串行化，可安全并发调用。
"""

from __future__ import annotations

import json
import sys
import threading
import time
from dataclasses import dataclass
from typing import TextIO


@dataclass
class AuditEvent:
    timestamp: float
    event: str
    client_id: str | None = None
    remote: str | None = None
    username: str | None = None
    reason: str | None = None
    outcome: str | None = None


class AuditLogger:
    def __init__(self, stream: TextIO | None = None, *, owns_stream: bool = False) -> None:
        self._stream: TextIO = stream if stream is not None else sys.stderr
        self._owns_stream = owns_stream
        self._lock = threading.Lock()

    def log(self, event: str, *, client_id: str | None = None,
            remote: str | None = None, username: str | None = None,
            reason: str | None = None, outcome: str | None = None) -> None:
        record = {
            "ts": time.time(),
            "event": event,
            "outcome": outcome,
            "client_id": client_id,
            "username": username,
            "remote": remote,
            "reason": reason,
        }
        # 去除值为 None 的字段，保持输出稳定。
        record = {key: value for key, value in record.items() if value is not None}
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()

    # 常用事件封装，统一事件名，避免调用方拼写分歧。
    def connect_accepted(self, *, client_id: str, remote: str | None,
                         username: str | None, clean_session: bool) -> None:
        self.log("connect", outcome="accepted", client_id=client_id,
                 remote=remote, username=username,
                 reason="clean" if clean_session else "persistent")

    def connect_rejected(self, *, client_id: str | None, remote: str | None,
                         username: str | None, reason: str) -> None:
        self.log("connect", outcome="rejected", client_id=client_id,
                 remote=remote, username=username, reason=reason)

    def disconnect_event(self, *, client_id: str, remote: str | None,
                         graceful: bool) -> None:
        self.log("disconnect",
                 outcome="graceful" if graceful else "abnormal",
                 client_id=client_id, remote=remote)

    def close(self) -> None:
        if self._owns_stream and self._stream is not sys.stderr:
            with self._lock:
                self._stream.close()
