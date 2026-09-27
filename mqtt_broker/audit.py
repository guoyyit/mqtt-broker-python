"""Append-only audit log for connection / auth events."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path


class AuditLogger:
    """Writes JSON-lines audit events; also keeps them in memory for tests."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path
        self._lock = threading.Lock()
        self.events: list[dict] = []
        self._fh = open(path, "a", encoding="utf-8") if path else None

    def log(self, event: str, **fields) -> None:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **fields,
        }
        line = json.dumps(record, ensure_ascii=False)
        with self._lock:
            self.events.append(record)
            if self._fh:
                self._fh.write(line + "\n")
                self._fh.flush()

    def close(self) -> None:
        with self._lock:
            if self._fh:
                self._fh.close()
                self._fh = None
