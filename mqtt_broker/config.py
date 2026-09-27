"""Broker 配置。

可通过环境变量覆盖（均为可选项）：

- ``MQTT_HOST`` / ``MQTT_PORT``：监听地址，默认 127.0.0.1:1883
- ``MQTT_CREDENTIALS``：``user:pass[,user:pass]`` 形式的账号表；
  留空表示允许匿名接入
- ``MQTT_DATA_DIR``：持久化目录；不设置时纯内存运行（无崩溃恢复）
- ``MQTT_RETRY_INTERVAL``：未确认下行 QoS1/2 报文重传间隔（秒）
- ``MQTT_AUDIT_LOG``：审计事件输出文件路径；不设置时写入 stderr
- ``MQTT_NOSYNC``：置为 ``1`` 时关闭 WAL fsync（仅测试用，会削弱崩溃语义）
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class BrokerConfig:
    host: str = "127.0.0.1"
    port: int = 1883
    # 用户名 -> 口令；为空表示允许匿名。
    credentials: dict[str, str] = field(default_factory=dict)
    # 持久化目录；None 表示纯内存运行。
    data_dir: str | None = None
    # 未确认下行报文的重传间隔（秒）。
    retry_interval: float = 5.0
    # 事件审计输出文件；None 表示写入标准错误。
    audit_log: str | None = None
    # 写入 WAL 后是否 fsync（崩溃恢复语义要求 True）。
    fsync: bool = True

    @property
    def anonymous_allowed(self) -> bool:
        return len(self.credentials) == 0

    def persistence_path(self) -> Path | None:
        if self.data_dir is None:
            return None
        path = Path(self.data_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path


def _parse_credentials(raw: str) -> dict[str, str]:
    credentials: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" not in entry:
            raise ValueError("MQTT_CREDENTIALS entries must be 'username:password'")
        username, password = entry.split(":", 1)
        credentials[username] = password
    return credentials


def load_config() -> BrokerConfig:
    """从环境变量加载配置；未设置时使用仅监听回环地址的安全默认值。"""
    config = BrokerConfig()
    config.host = os.environ.get("MQTT_HOST", config.host)
    if port := os.environ.get("MQTT_PORT"):
        config.port = int(port)
    if raw := os.environ.get("MQTT_CREDENTIALS"):
        config.credentials = _parse_credentials(raw)
    data_dir = os.environ.get("MQTT_DATA_DIR")
    config.data_dir = data_dir or None
    if interval := os.environ.get("MQTT_RETRY_INTERVAL"):
        config.retry_interval = float(interval)
    audit_log = os.environ.get("MQTT_AUDIT_LOG")
    config.audit_log = audit_log or None
    if os.environ.get("MQTT_NOSYNC") == "1":
        config.fsync = False
    return config
