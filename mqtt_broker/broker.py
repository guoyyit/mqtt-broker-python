"""Broker 核心状态机。

并发模型：
- 单把异步锁 ``self.lock`` 保护会话表、在线表、订阅集合与保留消息索引，
  使“发布/订阅/重连/保留消息更新”对共享状态的变更互斥且确定；
- 每条连接自带 ``write_lock`` 串行化该连接的下行字节，保证同一连接内
  消息顺序；不同连接之间只保证“在锁内确定匹配集合与事件顺序”，不承诺
  跨连接的全局接收先后（TCP 独立通道）。

持久化模型（启用 data_dir 时）：
- 所有会改变持久状态（订阅/会话缓存/在途事务/保留消息/入站 QoS2 事务）
  的事件先写 WAL 并 fsync，之后才发送协议确认，因此“已确认即已持久”；
- 一次发布产生的多条事件作为一个批次单次 write+fsync，崩溃时要么整批
  可见、要么都不可见，不会出现“已缓存部分订阅”的中间状态；
- 快照经临时文件 + os.replace 原子落盘，重启先载快照再重放 WAL，
  重启过程不对外提供服务（start 返回前完成恢复），不暴露中间状态。
"""

from __future__ import annotations

import asyncio
import base64
import uuid
from typing import Any, Optional

from . import packet
from .audit import AuditLogger
from .config import BrokerConfig
from .packet import ConnectPacket, PublishPacket, SubscribePacket
from .session import Session, StoredMessage, Subscription, WillMessage
from .storage import EventStore
from .topic import TopicError, topic_matches, validate_filter, validate_publish_topic

# 每写入多少条事件做一次快照。
_CHECKPOINT_EVERY = 256


class Broker:
    def __init__(self, config: BrokerConfig) -> None:
        self.config = config
        self.audit = self._open_audit()
        self.store: Optional[EventStore] = None
        self.sessions: dict[str, Session] = {}
        self.connections: dict[str, object] = {}
        self.retained: dict[str, tuple[bytes, int]] = {}
        self.lock = asyncio.Lock()
        self._stopping = False
        self._events_since_checkpoint = 0
        self._retry_task: Optional[asyncio.Task] = None

    def _open_audit(self) -> AuditLogger:
        path = self.config.audit_log
        if path is None:
            return AuditLogger()
        return AuditLogger(open(path, "a", encoding="utf-8"), owns_stream=True)

    # ------------------------------------------------------------------
    # 生命周期与恢复
    # ------------------------------------------------------------------
    async def start(self) -> None:
        path = self.config.persistence_path()
        if path is not None:
            self.store = EventStore(path, fsync=self.config.fsync)
            self._recover()
        self._retry_task = asyncio.create_task(self._retry_loop())

    async def _retry_loop(self) -> None:
        """按配置间隔重传未确认的下行 QoS1/2（DUP=1，同一 packet id）。"""
        interval = max(0.1, float(self.config.retry_interval))
        try:
            while not self._stopping:
                await asyncio.sleep(min(interval, 1.0))
                if self._stopping:
                    break
                await self._retry_due(interval)
        except asyncio.CancelledError:
            pass

    async def _retry_due(self, interval: float) -> None:
        loop = asyncio.get_running_loop()
        due: list[tuple[Any, StoredMessage]] = []
        async with self.lock:
            now = loop.time()
            for session in self.sessions.values():
                if not session.online:
                    continue
                for msg in session.unacked_out.values():
                    if msg.sent_at is not None and now - msg.sent_at >= interval:
                        due.append((session.connection, msg))
                        msg.sent_at = now
        for conn, msg in due:
            if conn is not None:
                await self._write_stored(conn, msg, dup=True, retain=msg.retain)

    async def shutdown(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        if self._retry_task is not None:
            self._retry_task.cancel()
            try:
                await self._retry_task
            except asyncio.CancelledError:
                pass
        if self.store is not None:
            # 优雅关闭：落一次快照，缩短下次重放。
            async with self.lock:
                self.store.checkpoint(self._snapshot_state())
            self.store.close()
            self.store = None
        self.audit.close()

    def _persist(self, records: list[dict[str, Any]]) -> None:
        """锁内调用：批量追加事件（无事件时为空操作）。"""
        if self.store is None or not records:
            return
        self.store.append_many(records)
        self._events_since_checkpoint += len(records)
        if self._events_since_checkpoint >= _CHECKPOINT_EVERY:
            self.store.checkpoint(self._snapshot_state())
            self._events_since_checkpoint = 0

    def _recover(self) -> None:
        assert self.store is not None
        snapshot = self.store.snapshot_state
        if snapshot is not None:
            self._restore_snapshot(snapshot)
        self.store.replay(self._apply_event)
        self._finalize_recovery()

    def _finalize_recovery(self) -> None:
        """重放结束后的离线归位：所有未确认会话消息按 mid 回到离线队列，
        重连后按原顺序以 DUP（若曾分配 packet id）重传。"""
        for session in self.sessions.values():
            if session.clean_session:
                continue
            session.online = False
            session.connection = None
            messages = list(session.recovered_msgs.values()) + list(session.unacked_out.values())
            session.outbound = sorted(
                {m.mid: m for m in messages if m.mid is not None}.values(),
                key=lambda m: m.mid)
            session.unacked_out.clear()
            session.recovered_msgs.clear()

    def _apply_event(self, record: dict[str, Any]) -> None:
        event = record["event"]
        handler = _RECOVERY_EVENTS.get(event)
        if handler is not None:
            handler(self, record)

    # -- 快照序列化 -----------------------------------------------------
    def _snapshot_state(self) -> dict[str, Any]:
        sessions: dict[str, Any] = {}
        for client_id, session in self.sessions.items():
            if session.clean_session:
                continue
            sessions[client_id] = {
                "next_packet_id": session.next_packet_id,
                "next_mid": session.next_mid,
                "subscriptions": [
                    {"filter": s.topic_filter, "qos": s.qos}
                    for s in session.subscriptions.values()
                ],
                "messages": [
                    {"mid": m.mid, "topic": m.topic,
                     "payload": base64.b64encode(m.payload).decode("ascii"),
                     "qos": m.qos, "packet_id": m.packet_id}
                    for m in list(session.outbound) + list(session.unacked_out.values())
                ],
                "inbound_qos2": list(session.inbound_qos2.keys()),
                "will": (None if session.will is None else
                         {"topic": session.will.topic,
                          "payload": base64.b64encode(session.will.payload).decode("ascii"),
                          "qos": session.will.qos, "retain": session.will.retain}),
            }
        return {
            "sessions": sessions,
            "retained": [
                {"topic": topic, "payload": base64.b64encode(payload).decode("ascii"),
                 "qos": qos}
                for topic, (payload, qos) in self.retained.items()
            ],
        }

    def _restore_snapshot(self, state: dict[str, Any]) -> None:
        for client_id, blob in state.get("sessions", {}).items():
            session = Session(client_id=client_id, clean_session=False, online=False)
            session.next_packet_id = blob["next_packet_id"]
            session.next_mid = blob.get("next_mid", 0)
            for sub in blob["subscriptions"]:
                session.subscriptions[sub["filter"]] = Subscription(sub["filter"], sub["qos"])
            # 快照时刻的未确认消息统一进恢复索引，finalize 时归位离线队列。
            for item in blob.get("messages", []):
                msg = StoredMessage(
                    mid=item["mid"], topic=item["topic"],
                    payload=base64.b64decode(item["payload"]), qos=item["qos"],
                    packet_id=item.get("packet_id"))
                session.recovered_msgs[item["mid"]] = msg
            for pid in blob.get("inbound_qos2", []):
                session.inbound_qos2[pid] = "rec_sent"
            will = blob.get("will")
            if will is not None:
                session.will = WillMessage(
                    will["topic"], base64.b64decode(will["payload"]),
                    will["qos"], will["retain"])
            self.sessions[client_id] = session
        for item in state.get("retained", []):
            self.retained[item["topic"]] = (base64.b64decode(item["payload"]),
                                            item["qos"])

    # ------------------------------------------------------------------
    # 鉴权与连接
    # ------------------------------------------------------------------
    async def authenticate(self, username: Optional[str],
                           password: Optional[bytes]) -> bool:
        if self.config.anonymous_allowed:
            return True
        if username is None or password is None:
            return False
        expected = self.config.credentials.get(username)
        if expected is None:
            return False
        return expected.encode("utf-8") == password

    async def handle_connect(self, conn, connect: ConnectPacket) -> bool:
        remote = conn.remote
        username = connect.username
        async with self.lock:
            if not await self.authenticate(username, connect.password):
                reason = ("missing_credentials" if username is None
                          else "invalid_credentials")
                code = (packet.ConnectReturnCode.BAD_USERNAME_PASSWORD
                        if username is not None
                        else packet.ConnectReturnCode.NOT_AUTHORIZED)
                self.audit.connect_rejected(
                    client_id=connect.client_id or None, remote=remote,
                    username=username, reason=reason)
                await conn.send(packet.encode_connack(False, int(code)))
                return False

            client_id = connect.client_id
            if client_id == "":
                client_id = f"auto-{uuid.uuid4().hex}"

            session_present = False
            events: list[dict[str, Any]] = []
            existing = self.sessions.get(client_id)
            if connect.clean_session:
                if existing is not None and not existing.clean_session:
                    # clean 重连：整体丢弃旧持久会话。
                    events.append({"event": "session_delete", "client_id": client_id})
                    self._discard_session(existing)
                if existing is not None:
                    self._discard_session(existing)
                session = Session(client_id=client_id, clean_session=True)
                self.sessions[client_id] = session
            elif existing is None or existing.clean_session:
                session = Session(client_id=client_id, clean_session=False)
                self.sessions[client_id] = session
                events.append({"event": "session_create", "client_id": client_id})
            else:
                session = existing
                session_present = True

            # 无论 clean 还是持久会话，同 client id 已有在线连接一律踢除
            # （MQTT 3.1.1 §3.1.4）；放在统一位置避免分支遗漏。
            old_conn = self.connections.get(client_id)
            if old_conn is not None and old_conn is not conn:
                old_conn.graceful_close = False
                old_conn.takeover = True
                if existing is not None:
                    existing.connection = None
                await old_conn.close()

            session.online = True
            session.connection = conn
            if connect.will_topic is not None:
                session.will = WillMessage(
                    topic=connect.will_topic, payload=connect.will_message,
                    qos=connect.will_qos, retain=connect.will_retain)
                if not connect.clean_session:
                    events.append({"event": "will_set", "client_id": client_id,
                                   "topic": connect.will_topic,
                                   "payload": _b64(connect.will_message),
                                   "qos": connect.will_qos,
                                   "retain": connect.will_retain})
            else:
                session.will = None
            self.connections[client_id] = conn
            conn.client_id = client_id
            conn.session = session
            conn.keep_alive = connect.keep_alive
            conn.clean_session = connect.clean_session

            # CONNACK 前确保持久会话建立事件已落盘。
            self._persist(events)
            await conn.send(packet.encode_connack(session_present, 0))
            self.audit.connect_accepted(
                client_id=client_id, remote=remote, username=username,
                clean_session=connect.clean_session)

        if session_present:
            await self.flush_session(conn)
        return True

    def _discard_session(self, session: Session) -> None:
        session.subscriptions.clear()
        session.outbound.clear()
        session.unacked_out.clear()
        session.inbound_qos2.clear()
        session.will = None

    async def handle_disconnect(self, conn, *, graceful: bool) -> None:
        client_id = conn.client_id
        if client_id is None:
            if not graceful:
                self.audit.connect_rejected(
                    client_id=None, remote=getattr(conn, "remote", None),
                    username=None, reason="connection_closed_before_connect")
            return
        async with self.lock:
            # 该连接已被同 client id 的新连接接管：其收尾不得触碰新连接状态，
            # 也不得发布遗嘱（§3.1.4 踢除不等同异常断开）。
            if getattr(conn, "takeover", False):
                self.audit.disconnect_event(
                    client_id=client_id, remote=conn.remote, graceful=True)
                return
            session = self.sessions.get(client_id)
            # 在线表若已不属于本连接，同样跳过（双重收尾保护）。
            current = self.connections.get(client_id)
            if current is not None and current is not conn:
                return
            self.connections.pop(client_id, None)
            if session is None:
                return
            will = session.will
            events: list[dict[str, Any]] = []
            if graceful and will is not None and not session.clean_session:
                events.append({"event": "will_clear", "client_id": client_id})
            if conn.clean_session or session.clean_session:
                if not session.clean_session:
                    events.append({"event": "session_delete", "client_id": client_id})
                self._discard_session(session)
                self.sessions.pop(client_id, None)
            else:
                session.online = False
                session.connection = None
                if graceful:
                    session.will = None
                # 在途未确认消息退回队首，重连后以 DUP 重传（保留 packet id）。
                for pid in sorted(session.unacked_out, reverse=True):
                    msg = session.unacked_out.pop(pid)
                    session.outbound.insert(0, msg)
            self._persist(events)
            self.audit.disconnect_event(
                client_id=client_id, remote=conn.remote, graceful=graceful)

        if not graceful and will is not None and not self._stopping:
            # 遗嘱只发布一次：先持久清除，再走正常发布路径。
            async with self.lock:
                if session is not None and not session.clean_session:
                    self._persist([{"event": "will_clear",
                                    "client_id": client_id}])
            await self._publish_will(client_id, will)

    async def _publish_will(self, owner_id: str, will: WillMessage) -> None:
        async with self.lock:
            session = self.sessions.get(owner_id)
            if session is not None:
                # 已发布的遗嘱在内存中也失效，重连后不会复活。
                session.will = None
        # 遗嘱走与普通 PUBLISH 完全一致的匹配/持久化路径。
        pub = PublishPacket(topic=will.topic, payload=will.payload,
                            qos=will.qos, retain=will.retain)
        await self._route_publish(pub, publisher_id=owner_id)

    # ------------------------------------------------------------------
    # 订阅 / 退订
    # ------------------------------------------------------------------
    async def handle_subscribe(self, conn, sub: SubscribePacket) -> list[int]:
        codes: list[int] = []
        async with self.lock:
            session: Session = conn.session
            new_filters: list[str] = []
            events: list[dict[str, Any]] = []
            for topic_filter, qos in sub.topics:
                try:
                    validate_filter(topic_filter)
                except TopicError:
                    codes.append(packet.QoS.FAILURE)
                    continue
                granted = min(qos, 2)
                session.subscriptions[topic_filter] = Subscription(topic_filter, granted)
                new_filters.append(topic_filter)
                codes.append(granted)
                if not session.clean_session:
                    events.append({"event": "subscribe", "client_id": conn.client_id,
                                   "filter": topic_filter, "qos": granted})
            # SUBACK 前订阅已持久。
            self._persist(events)
            retained_hits: list[tuple[str, bytes, int]] = []
            for topic_filter in new_filters:
                for rtopic, (rpayload, rqos) in self.retained.items():
                    if topic_matches(topic_filter, rtopic):
                        retained_hits.append((rtopic, rpayload, rqos))
        for rtopic, rpayload, rqos in retained_hits:
            await self._send_retained(session, rtopic, rpayload, rqos)
        return codes

    async def handle_unsubscribe(self, conn, topics: list[str]) -> None:
        async with self.lock:
            session: Session = conn.session
            events: list[dict[str, Any]] = []
            for topic_filter in topics:
                if session.subscriptions.pop(topic_filter, None) is not None \
                        and not session.clean_session:
                    events.append({"event": "unsubscribe",
                                   "client_id": conn.client_id, "filter": topic_filter})
            self._persist(events)

    # ------------------------------------------------------------------
    # 入站发布与路由
    # ------------------------------------------------------------------
    async def handle_publish(self, conn, pub: PublishPacket) -> None:
        try:
            validate_publish_topic(pub.topic)
        except TopicError:
            await conn.close()
            return
        await self._route_publish(pub, publisher_session=conn.session, conn=conn)

    async def _route_publish(self, pub: PublishPacket, *,
                             publisher_session: Optional[Session] = None,
                             conn=None, publisher_id: Optional[str] = None,
                             deliver_to_subscribers: bool = True) -> None:
        """锁内完成状态变更与持久化事件组装，锁外完成网络写。

        ``deliver_to_subscribers`` 为 False 时仅更新保留存储（当前无此场景）；
        普通保留 PUBLISH 按 §3.3.1-3 发布时不投递给既有订阅者；遗嘱消息是
        该规则的例外：retain 置位的遗嘱同时更新保留存储并投递给匹配订阅者。
        """
        async with self.lock:
            events: list[dict[str, Any]] = []
            publisher_id = publisher_id or (
                publisher_session.client_id if publisher_session else None)

            # 入站 QoS2 事务记账（仅在“新”PUBLISH 时路由；重传只重发确认）。
            duplicate_inbound = False
            if pub.qos == 2 and publisher_session is not None:
                if pub.packet_id in publisher_session.inbound_qos2:
                    duplicate_inbound = True
                else:
                    publisher_session.inbound_qos2[pub.packet_id] = "rec_sent"
                    events.append({"event": "inbound_rec",
                                   "client_id": publisher_session.client_id,
                                   "packet_id": pub.packet_id})

            # 保留消息处理。
            if pub.retain:
                if pub.payload == b"":
                    if pub.topic in self.retained:
                        self.retained.pop(pub.topic)
                        events.append({"event": "retained_clear", "topic": pub.topic})
                else:
                    self.retained[pub.topic] = (pub.payload, pub.qos)
                    events.append({"event": "retained_set", "topic": pub.topic,
                                   "payload": _b64(pub.payload), "qos": pub.qos})

            # 匹配订阅者并把消息放入各持久会话（离线缓存或在途表）。
            # 普通保留 PUBLISH：发布时不投递给既有订阅者（§3.3.1-3）；
            # 遗嘱消息除外，仍按普通消息投递（§3.1.2-10）。
            suppress_delivery = pub.retain and publisher_session is not None
            sends: list[tuple[Any, StoredMessage]] = []
            if (deliver_to_subscribers and not duplicate_inbound
                    and not suppress_delivery):
                targets = self._match_subscribers(pub.topic, pub.qos,
                                                  exclude=publisher_id)
                for session, effective_qos in targets:
                    sends.extend(self._ingress_for_session(
                        session, pub.topic, pub.payload, effective_qos, events))

            # 单批次落盘：之后才允许任何协议确认与下行发送。
            self._persist(events)

            if publisher_session is None:
                publisher_ack: Optional[bytes] = None
            elif pub.qos == 2:
                publisher_ack = packet.encode_ack(packet.PUBREC, pub.packet_id)
            elif pub.qos == 1:
                publisher_ack = packet.encode_ack(packet.PUBACK, pub.packet_id)
            else:
                publisher_ack = None

        # ---- 锁外：网络 I/O -------------------------------------------
        if publisher_ack is not None and conn is not None:
            await conn.send(publisher_ack)
        for target_conn, msg in sends:
            await self._write_stored(target_conn, msg, dup=False)

    def _ingress_for_session(self, session: Session, topic: str,
                             payload: bytes, qos: int,
                             events: list[dict[str, Any]]) -> list[tuple[Any, StoredMessage]]:
        """锁内：把一条匹配消息登记到会话，并产生持久事件与待发送列表。"""
        conn = session.connection if session.online else None
        if qos == 0:
            if conn is not None:
                msg = StoredMessage(topic=topic, payload=payload, qos=0)
                return [(conn, msg)]
            return []
        mid = session.next_mid
        session.next_mid += 1
        msg = StoredMessage(mid=mid, topic=topic, payload=payload, qos=qos)
        events.append({"event": "msg_enqueued", "client_id": session.client_id,
                       "mid": mid, "topic": topic,
                       "payload": _b64(payload), "qos": qos})
        if conn is None:
            if not session.clean_session:
                session.outbound.append(msg)
            return []
        packet_id = session.allocate_packet_id()
        msg.packet_id = packet_id
        session.unacked_out[packet_id] = msg
        events.append({"event": "msg_inflight", "client_id": session.client_id,
                       "mid": mid, "packet_id": packet_id})
        return [(conn, msg)]

    async def _write_stored(self, conn, msg: StoredMessage, *, dup: bool,
                            retain: bool = False) -> None:
        msg.sent_at = asyncio.get_running_loop().time()
        data = packet.encode_publish(PublishPacket(
            topic=msg.topic, payload=msg.payload, qos=msg.qos,
            retain=retain, dup=dup, packet_id=msg.packet_id))
        await conn.send(data)

    # ------------------------------------------------------------------
    # QoS 确认
    # ------------------------------------------------------------------
    async def handle_puback(self, conn, packet_id: int) -> None:
        async with self.lock:
            self._confirm_outbound(conn.session, packet_id)

    async def handle_pubrec(self, conn, packet_id: int) -> None:
        # 下行 QoS2：客户端 PUBREC -> 持久确认后回 PUBREL。
        async with self.lock:
            self._confirm_outbound(conn.session, packet_id)
        await conn.send(packet.encode_ack(packet.PUBREL, packet_id))

    def _confirm_outbound(self, session: Session, packet_id: int) -> None:
        msg = session.unacked_out.pop(packet_id, None)
        if msg is not None and msg.mid is not None and not session.clean_session:
            self._persist([{"event": "msg_acked", "client_id": session.client_id,
                            "mid": msg.mid}])

    async def handle_pubrel(self, conn, packet_id: int) -> None:
        async with self.lock:
            session: Session = conn.session
            if packet_id in session.inbound_qos2:
                session.inbound_qos2.pop(packet_id)
                if not session.clean_session:
                    self._persist([{"event": "inbound_rel",
                                    "client_id": session.client_id,
                                    "packet_id": packet_id}])
        await conn.send(packet.encode_ack(packet.PUBCOMP, packet_id))

    async def handle_pubcomp(self, conn, packet_id: int) -> None:
        async with self.lock:
            self._confirm_outbound(conn.session, packet_id)

    # ------------------------------------------------------------------
    # 匹配与发送
    # ------------------------------------------------------------------
    def _match_subscribers(self, topic: str, publish_qos: int,
                           exclude: Optional[str] = None
                           ) -> list[tuple[Session, int]]:
        result: list[tuple[Session, int]] = []
        for client_id, session in self.sessions.items():
            # 匹配依据是订阅集合而非在线状态；离线持久会话匹配后进入会话缓存。
            if client_id == exclude:
                continue
            best_qos: Optional[int] = None
            for sub in session.subscriptions.values():
                if topic_matches(sub.topic_filter, topic):
                    effective = min(publish_qos, sub.qos)
                    if best_qos is None or effective > best_qos:
                        best_qos = effective
            if best_qos is not None:
                result.append((session, best_qos))
        return result

    async def _send_retained(self, session: Session, topic: str,
                             payload: bytes, published_qos: int) -> None:
        """新订阅即时投递保留消息：按订阅 QoS 降级，retain 标志置位。"""
        best_qos = 0
        for sub in session.subscriptions.values():
            if topic_matches(sub.topic_filter, topic):
                best_qos = max(best_qos, min(published_qos, sub.qos))
        conn = session.connection if session.online else None
        if conn is None:
            return
        if best_qos == 0:
            await conn.send(packet.encode_publish(PublishPacket(
                topic=topic, payload=payload, qos=0, retain=True)))
            return
        async with self.lock:
            packet_id = session.allocate_packet_id()
            msg = StoredMessage(topic=topic, payload=payload, qos=best_qos,
                                retain=True, packet_id=packet_id)
            session.unacked_out[packet_id] = msg
        await self._write_stored(conn, msg, dup=False, retain=True)

    async def flush_session(self, conn) -> None:
        """重连后下发离线缓存与重传未确认消息（按队列原顺序）。"""
        async with self.lock:
            session: Session = conn.session
            queued = session.outbound
            session.outbound = []
            sends: list[tuple[StoredMessage, bool]] = []
            events: list[dict[str, Any]] = []
            # 先为“保留旧 packet id”的在途重传占位，避免新分配与旧 id 冲突。
            for msg in queued:
                if msg.qos > 0 and msg.packet_id is not None:
                    session.unacked_out[msg.packet_id] = msg
            for msg in queued:
                if msg.qos == 0:
                    sends.append((msg, False))
                    continue
                dup = msg.packet_id is not None
                if msg.packet_id is None:
                    msg.packet_id = session.allocate_packet_id()
                    session.unacked_out[msg.packet_id] = msg
                    events.append({"event": "msg_inflight",
                                   "client_id": session.client_id,
                                   "mid": msg.mid, "packet_id": msg.packet_id})
                sends.append((msg, dup))
            self._persist(events)
        for msg, dup in sends:
            if msg.qos == 0:
                await conn.send(packet.encode_publish(PublishPacket(
                    topic=msg.topic, payload=msg.payload, qos=0)))
            else:
                await self._write_stored(conn, msg, dup=dup)


def _b64(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


# ---------------------------------------------------------------------------
# WAL 事件恢复
# ---------------------------------------------------------------------------
def _ev_session_create(broker: Broker, r: dict[str, Any]) -> None:
    client_id = r["client_id"]
    if client_id not in broker.sessions:
        broker.sessions[client_id] = Session(client_id=client_id, clean_session=False)


def _ev_session_delete(broker: Broker, r: dict[str, Any]) -> None:
    broker.sessions.pop(r["client_id"], None)


def _ev_subscribe(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is not None:
        session.subscriptions[r["filter"]] = Subscription(r["filter"], r["qos"])


def _ev_unsubscribe(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is not None:
        session.subscriptions.pop(r["filter"], None)


def _ev_will_set(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is not None:
        session.will = WillMessage(r["topic"], base64.b64decode(r["payload"]),
                                   r["qos"], r["retain"])


def _ev_will_clear(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is not None:
        session.will = None


def _ev_retained_set(broker: Broker, r: dict[str, Any]) -> None:
    broker.retained[r["topic"]] = (base64.b64decode(r["payload"]), r["qos"])


def _ev_retained_clear(broker: Broker, r: dict[str, Any]) -> None:
    broker.retained.pop(r["topic"], None)


def _ev_msg_enqueued(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is None:
        return
    msg = StoredMessage(mid=r["mid"], topic=r["topic"],
                        payload=base64.b64decode(r["payload"]), qos=r["qos"])
    session.recovered_msgs[r["mid"]] = msg
    session.next_mid = max(session.next_mid, r["mid"] + 1)


def _ev_msg_inflight(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is None:
        return
    msg = session.recovered_msgs.pop(r["mid"], None)
    if msg is None:
        # 可能来自快照后的重复重放边界：忽略。
        return
    msg.packet_id = r["packet_id"]
    session.unacked_out[r["packet_id"]] = msg
    session.next_packet_id = max(session.next_packet_id,
                                 (r["packet_id"] % 65535) + 1)


def _ev_msg_acked(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is None:
        return
    # 仍在离线队列中的消息收到确认（理论不会发生）：直接移除。
    session.recovered_msgs.pop(r["mid"], None)
    # 在途表中按 mid 反查 packet id 后移除。
    for pid, inflight in list(session.unacked_out.items()):
        if inflight.mid == r["mid"]:
            session.unacked_out.pop(pid)


def _ev_inbound_rec(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is not None:
        session.inbound_qos2[r["packet_id"]] = "rec_sent"


def _ev_inbound_rel(broker: Broker, r: dict[str, Any]) -> None:
    session = broker.sessions.get(r["client_id"])
    if session is not None:
        session.inbound_qos2.pop(r["packet_id"], None)


_RECOVERY_EVENTS = {
    "session_create": _ev_session_create,
    "session_delete": _ev_session_delete,
    "subscribe": _ev_subscribe,
    "unsubscribe": _ev_unsubscribe,
    "will_set": _ev_will_set,
    "will_clear": _ev_will_clear,
    "retained_set": _ev_retained_set,
    "retained_clear": _ev_retained_clear,
    "msg_enqueued": _ev_msg_enqueued,
    "msg_inflight": _ev_msg_inflight,
    "msg_acked": _ev_msg_acked,
    "inbound_rec": _ev_inbound_rec,
    "inbound_rel": _ev_inbound_rel,
}
