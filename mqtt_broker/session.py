"""客户端会话状态：订阅、离线缓存、QoS 事务。

调用约定：对 Session 内部集合的读写都在 Broker 的全局 ``lock`` 下进行，
因此本类自身不再加锁；包 ID 在单个会话（即单个客户端）命名空间当前
未确认集合内循环分配（MQTT 3.1.1 §2.3.1）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

MAX_PACKET_ID = 65535


@dataclass
class WillMessage:
    topic: str
    payload: bytes
    qos: int
    retain: bool


@dataclass
class StoredMessage:
    """等待下发的消息。

    mid 是会话内单调递增的持久化消息序号；QoS0 与保留消息即时下发不分配 mid。
    """
    topic: str
    payload: bytes
    qos: int
    mid: Optional[int] = None
    retain: bool = False
    packet_id: Optional[int] = None
    # 最近一次下行发送的事件循环时间，用于服务端重传判定。
    sent_at: Optional[float] = None


@dataclass
class Subscription:
    topic_filter: str
    qos: int


@dataclass
class Session:
    client_id: str
    clean_session: bool
    subscriptions: dict[str, Subscription] = field(default_factory=dict)
    # 尚未下发的下行会话消息队列（含离线期间缓存的消息）。
    outbound: list[StoredMessage] = field(default_factory=list)
    # 已下发但未被客户端确认：packet_id -> StoredMessage。
    unacked_out: dict[int, StoredMessage] = field(default_factory=dict)
    # 入站 QoS2 事务：packet_id -> "rec_sent"（已回 PUBREC，等待 PUBREL）。
    inbound_qos2: dict[int, str] = field(default_factory=dict)
    next_packet_id: int = 1
    next_mid: int = 1
    will: Optional[WillMessage] = None
    online: bool = False
    # 关联连接对象，离线时为 None；类型用 object 避免与 server.py 循环导入。
    connection: object = None
    # 仅恢复期间使用：mid -> StoredMessage，重放结束后剩余项并入 outbound。
    recovered_msgs: dict[int, StoredMessage] = field(default_factory=dict)

    def allocate_packet_id(self) -> int:
        """分配当前未在使用的包 ID；unacked 满载时抛出 OverflowError。"""
        start = self.next_packet_id
        candidate = start
        while True:
            if candidate not in self.unacked_out:
                self.next_packet_id = candidate + 1 if candidate < MAX_PACKET_ID else 1
                return candidate
            candidate += 1
            if candidate > MAX_PACKET_ID:
                candidate = 1
            if candidate == start:
                raise OverflowError("no free packet id in session")

    def enqueue(self, message: StoredMessage) -> None:
        self.outbound.append(message)
