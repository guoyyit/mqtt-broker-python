"""崩溃恢复持久化：JSONL 预写日志（WAL）+ 原子快照替换。

约定：
- 每次 :meth:`EventStore.append` 先落盘并 fsync（可配置关闭）才返回，
  因此“已返回”即“已持久”，未完成的事务绝不会先被标记为确认；
- 快照写入同目录临时文件后 ``os.replace`` 原子替换，崩溃时要么看到旧快照
  要么看到新快照，重启不会暴露中间状态；
- 快照之后、WAL 截断之前的崩溃窗口靠序列号去重：重放时忽略
  ``seq <= snapshot_seq`` 的日志记录。
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Callable

WAL_NAME = "wal.log"
SNAPSHOT_NAME = "snapshot.json"
SNAPSHOT_TMP = "snapshot.json.tmp"


class EventStore:
    def __init__(self, data_dir: str | Path, *, fsync: bool = True) -> None:
        self._dir = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._fsync_enabled = fsync
        self._lock = threading.Lock()
        self._seq = 0
        self._snapshot_seq = 0
        self._snapshot_state: dict[str, Any] | None = None
        self._load_snapshot()
        wal_path = self._dir / WAL_NAME
        self._wal = open(wal_path, "a+b")
        # 统计需要重放的有效记录，恢复序列号水位。
        self._recover_seq_watermark()

    # ------------------------------------------------------------------
    # 启动恢复
    # ------------------------------------------------------------------
    def _load_snapshot(self) -> None:
        path = self._dir / SNAPSHOT_NAME
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as handle:
            blob = json.load(handle)
        self._snapshot_seq = int(blob["seq"])
        self._snapshot_state = blob["state"]
        self._seq = self._snapshot_seq

    def _recover_seq_watermark(self) -> None:
        wal_path = self._dir / WAL_NAME
        if not wal_path.exists():
            return
        with open(wal_path, "rb") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                self._seq = max(self._seq, int(record["seq"]))

    @property
    def snapshot_state(self) -> dict[str, Any] | None:
        return self._snapshot_state

    @property
    def next_seq(self) -> int:
        return self._seq + 1

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def append(self, event: str, **fields: Any) -> dict[str, Any]:
        return self.append_many([{"event": event, **fields}])[0]

    def append_many(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """批量追加：多条记录共用一次 write+fsync，原子跨越崩溃边界。"""
        if not records:
            return []
        with self._lock:
            materialized: list[dict[str, Any]] = []
            buffer = bytearray()
            for fields in records:
                self._seq += 1
                record = {**fields, "seq": self._seq}
                materialized.append(record)
                buffer += (json.dumps(record, ensure_ascii=False,
                                      separators=(",", ":")) + "\n").encode("utf-8")
            self._wal.write(buffer)
            self._wal.flush()
            if self._fsync_enabled:
                os.fsync(self._wal.fileno())
            return materialized

    def replay(self, callback: Callable[[dict[str, Any]], None]) -> None:
        """按顺序重放快照之后的 WAL 记录。"""
        with self._lock:
            wal_path = self._dir / WAL_NAME
            if not wal_path.exists():
                return
            with open(wal_path, "rb") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    if int(record["seq"]) <= self._snapshot_seq:
                        continue
                    callback(record)

    def checkpoint(self, state: dict[str, Any]) -> None:
        """用给定全量状态生成快照并截断已覆盖的 WAL。"""
        with self._lock:
            blob = {"seq": self._seq, "state": state}
            tmp_path = self._dir / SNAPSHOT_TMP
            final_path = self._dir / SNAPSHOT_NAME
            with open(tmp_path, "w", encoding="utf-8") as handle:
                json.dump(blob, handle, ensure_ascii=False)
                handle.flush()
                if self._fsync_enabled:
                    os.fsync(handle.fileno())
            os.replace(tmp_path, final_path)
            self._snapshot_seq = self._seq
            self._snapshot_state = state
            # WAL 已被快照覆盖，安全截断。
            self._wal.seek(0)
            self._wal.truncate()
            self._wal.flush()
            if self._fsync_enabled:
                os.fsync(self._wal.fileno())

    def close(self) -> None:
        with self._lock:
            try:
                self._wal.flush()
                if self._fsync_enabled:
                    os.fsync(self._wal.fileno())
            finally:
                self._wal.close()
