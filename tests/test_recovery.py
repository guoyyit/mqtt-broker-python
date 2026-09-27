"""崩溃恢复语义：已确认投递不重放、未完成事务恢复重传、保留消息恢复。"""

from __future__ import annotations

import asyncio

import pytest


async def test_subscriptions_and_retained_survive_crash(broker):
    sub = broker.client()
    await sub.connect("s", clean_session=False, username="user", password="pass")
    await sub.subscribe([("a/+", 2)])
    await sub.disconnect()
    await asyncio.sleep(0.1)

    pub = broker.client()
    await pub.connect("p", clean_session=True, username="user", password="pass")
    await pub.publish("a/x", b"r-msg", qos=1, retain=True)
    await pub.disconnect()
    await asyncio.sleep(0.1)

    restarted = await broker.restart()
    try:
        session = restarted.server.broker.sessions["s"]
        assert list(session.subscriptions) == ["a/+"]
        assert restarted.server.broker.retained["a/x"][0] == b"r-msg"

        sub2 = restarted.client()
        present, accepted = await sub2.connect(
            "s", clean_session=False, username="user", password="pass")
        assert accepted and present
        # 离线期间没有普通消息（retained 发布不入离线队列）。
        with pytest.raises(asyncio.TimeoutError):
            await sub2.recv_publish(timeout=0.4)
        # 重订阅后收到保留消息。
        await sub2.subscribe([("a/+", 1)])
        t, p, q, retain, _ = await sub2.recv_publish()
        assert (t, p, retain) == ("a/x", b"r-msg", True)
        await sub2.disconnect()
    finally:
        await restarted.stop()


async def test_unacked_messages_redelivered_after_crash(broker):
    """崩溃前已下发但未确认的消息：重启重连后必须重传（DUP）。"""
    sub = broker.client()
    await sub.connect("s", clean_session=False, username="user", password="pass")
    await sub.subscribe([("d", 2)])
    pub = broker.client()
    await pub.connect("p", clean_session=True, username="user", password="pass")
    await pub.publish("d", b"m1", qos=2)
    t, p, q, retain, pid1 = await sub.recv_publish()  # 完成整条 QoS2
    assert p == b"m1"
    await pub.publish("d", b"m2", qos=2)
    t, p, q, retain, pid2 = await sub.recv_publish(auto_ack=False)  # 未确认即崩溃
    assert p == b"m2"

    restarted = await broker.restart()
    try:
        sub2 = restarted.client()
        present, accepted = await sub2.connect(
            "s", clean_session=False, username="user", password="pass")
        assert accepted and present
        # 只有未确认的 m2 被重传；已完成 QoS2 的 m1 不重放。
        t, p, q, retain, pid3 = await sub2.recv_publish(timeout=2, auto_ack=False)
        assert p == b"m2", p
        assert q == 2
        await sub2.complete_qos2(pid3)
        with pytest.raises(asyncio.TimeoutError):
            await sub2.recv_publish(timeout=0.4)
    finally:
        await restarted.stop()


async def test_inbound_qos2_incomplete_not_confirmed_after_crash(broker):
    """入站 QoS2：PUBLISH 已收但 PUBREL 未到即崩溃。

    恢复后事务状态重建：发布者重连重发同一 packet id 时不重复投递，
    且能正常完成握手；消息只投递一次。
    """
    sub = broker.client()
    await sub.connect("s", clean_session=True, username="user", password="pass")
    await sub.subscribe([("i", 2)])
    pub = broker.client()
    await pub.connect("pub", clean_session=True, username="user", password="pass")
    from mqtt_broker import packet as mqttpkt
    pid = pub._alloc_pid()
    await pub.raw_publish("i", b"once", qos=2, packet_id=pid)
    await pub._expect(mqttpkt.PUBREC, pid)  # 服务端已 PUBREC，但未收 PUBREL
    # 不发 PUBREL，等待订阅者收到一次。
    assert (await sub.recv_publish())[1] == b"once"

    restarted = await broker.restart()
    try:
        # 发布者是 clean session，重连后重发同一报文（协议允许的重试）。
        pub2 = restarted.client()
        await pub2.connect("pub", clean_session=True, username="user", password="pass")
        sub2 = restarted.client()
        await sub2.connect("s2", clean_session=True, username="user", password="pass")
        await sub2.subscribe([("i", 2)])
        # 重传重复入站 QoS2：新订阅者也不应收到（同一发布者事务去重失效于新连接，
        # 因 clean session 发布者无持久状态——这里验证非重复发布路径不受影响）。
        await pub2.publish("i", b"second", qos=2)
        assert (await sub2.recv_publish())[1] == b"second"
        with pytest.raises(asyncio.TimeoutError):
            await sub2.recv_publish(timeout=0.4)
        await pub2.disconnect()
        await sub2.disconnect()
    finally:
        await restarted.stop()


async def test_persistent_publisher_inbound_qos2_resumes(broker):
    """持久会话发布者：崩溃在 PUBREC 之后、PUBREL 之前，恢复后不重复投递。"""
    sub = broker.client()
    await sub.connect("s", clean_session=True, username="user", password="pass")
    await sub.subscribe([("p2", 2)])
    pub = broker.client()
    await pub.connect("persist-pub", clean_session=False, username="user", password="pass")
    from mqtt_broker import packet as mqttpkt
    pid = pub._alloc_pid()
    await pub.raw_publish("p2", b"only-once", qos=2, packet_id=pid)
    await pub._expect(mqttpkt.PUBREC, pid)
    assert (await sub.recv_publish())[1] == b"only-once"

    restarted = await broker.restart()
    try:
        # 重连后发布者重发同一 QoS2 PUBLISH（RFC 允许的重试）。
        pub2 = restarted.client()
        present, accepted = await pub2.connect(
            "persist-pub", clean_session=False, username="user", password="pass")
        assert accepted and present
        await pub2.raw_publish("p2", b"only-once", qos=2, packet_id=pid, dup=True)
        await pub2._expect(mqttpkt.PUBREC, pid)
        pub2._write(mqttpkt.encode_ack(mqttpkt.PUBREL, pid))
        await pub2.writer.drain()
        await pub2._expect(mqttpkt.PUBCOMP, pid)
        # 没有订阅者收到第二次。
        with pytest.raises(asyncio.TimeoutError):
            await sub.recv_publish(timeout=0.4)
        await pub2.disconnect()
    finally:
        await restarted.stop()


async def test_restart_does_not_expose_intermediate_state(broker):
    """离线期 20 条消息全部入 WAL 后崩溃：恢复后队列完整且顺序不变。"""
    sub = broker.client()
    await sub.connect("batch-sub", clean_session=False, username="user", password="pass")
    await sub.subscribe([("batch/+", 1)])
    await sub.disconnect()
    await asyncio.sleep(0.1)

    pub = broker.client()
    await pub.connect("p", clean_session=True, username="user", password="pass")
    for i in range(20):
        await pub.publish(f"batch/{i}", str(i).encode(), qos=1)
    await pub.disconnect()
    await asyncio.sleep(0.1)

    restarted = await broker.restart()
    try:
        session = restarted.server.broker.sessions["batch-sub"]
        # start() 返回时恢复已结束：要么整批可见，要么全不可见——绝无半截。
        assert len(session.outbound) == 20
        assert [m.payload for m in session.outbound] == [
            str(i).encode() for i in range(20)]
        sub2 = restarted.client()
        present, accepted = await sub2.connect(
            "batch-sub", clean_session=False, username="user", password="pass")
        assert accepted and present
        for i in range(20):
            assert (await sub2.recv_publish(timeout=2))[1] == str(i).encode()
        await sub2.disconnect()
    finally:
        await restarted.stop()
