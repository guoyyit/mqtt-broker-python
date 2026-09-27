"""持久会话（离线缓存/订阅恢复/session present）与遗嘱消息。"""

from __future__ import annotations

import asyncio

import pytest


async def test_session_present_flag(broker):
    c = broker.client()
    present, accepted = await c.connect(
        "persist", clean_session=False, username="user", password="pass")
    assert accepted and present is False
    await c.disconnect()
    await asyncio.sleep(0.1)
    c2 = broker.client()
    present, accepted = await c2.connect(
        "persist", clean_session=False, username="user", password="pass")
    assert accepted and present is True
    await c2.disconnect()
    # clean session 连接后旧持久会话被删除。
    c3 = broker.client()
    present, accepted = await c3.connect(
        "persist", clean_session=True, username="user", password="pass")
    assert accepted and present is False
    await c3.disconnect()
    await asyncio.sleep(0.1)
    c4 = broker.client()
    present, accepted = await c4.connect(
        "persist", clean_session=False, username="user", password="pass")
    assert accepted and present is False
    await c4.disconnect()


async def test_offline_messages_restored_in_order(broker):
    sub = broker.client()
    await sub.connect("offline-c", clean_session=False, username="user", password="pass")
    await sub.subscribe([("dev/+/d", 2)])
    await sub.disconnect()
    await asyncio.sleep(0.1)

    pub = broker.client()
    await pub.connect("pub", clean_session=True, username="user", password="pass")
    await pub.publish("dev/1/d", b"m1", qos=1)
    await pub.publish("dev/1/d", b"m2", qos=2)
    await pub.publish("dev/1/d", b"m3", qos=1)
    # QoS0 离线消息不缓存。
    await pub.publish("dev/1/d", b"m0", qos=0)
    await pub.disconnect()

    sub2 = broker.client()
    present, accepted = await sub2.connect(
        "offline-c", clean_session=False, username="user", password="pass")
    assert accepted and present
    assert (await sub2.recv_publish())[1] == b"m1"
    assert (await sub2.recv_publish())[1] == b"m2"
    assert (await sub2.recv_publish())[1] == b"m3"
    with pytest.raises(asyncio.TimeoutError):
        await sub2.recv_publish(timeout=0.4)
    await sub2.disconnect()


async def test_subscriptions_restored_after_reconnect(broker):
    sub = broker.client()
    await sub.connect("sub-restore", clean_session=False, username="user", password="pass")
    await sub.subscribe([("a/+", 1), ("b/#", 2)])
    await sub.disconnect()
    await asyncio.sleep(0.1)

    pub = broker.client()
    await pub.connect("pub", clean_session=True, username="user", password="pass")
    await pub.publish("a/x", b"ax", qos=1)
    await pub.publish("b/c/d", b"bcd", qos=2)
    await pub.disconnect()

    sub2 = broker.client()
    await sub2.connect("sub-restore", clean_session=False, username="user", password="pass")
    got = {(await sub2.recv_publish())[1] for _ in range(2)}
    assert got == {b"ax", b"bcd"}
    await sub2.disconnect()


async def test_unsubscribe_persists(broker):
    sub = broker.client()
    await sub.connect("unsub-c", clean_session=False, username="user", password="pass")
    await sub.subscribe([("x/+", 1)])
    await sub.unsubscribe(["x/+"])
    await sub.disconnect()
    await asyncio.sleep(0.1)
    pub = broker.client()
    await pub.connect("pub", clean_session=True, username="user", password="pass")
    await pub.publish("x/1", b"x", qos=1)
    await pub.disconnect()
    sub2 = broker.client()
    await sub2.connect("unsub-c", clean_session=False, username="user", password="pass")
    with pytest.raises(asyncio.TimeoutError):
        await sub2.recv_publish(timeout=0.4)
    await sub2.disconnect()


async def test_will_published_on_hard_disconnect(broker):
    watcher = broker.client()
    await watcher.connect("w", clean_session=True, username="user", password="pass")
    await watcher.subscribe([("will/x", 1)])
    dying = broker.client()
    await dying.connect("dying", clean_session=True, username="user", password="pass",
                        will_topic="will/x", will_payload=b"bye", will_qos=1)
    await dying.hard_disconnect()
    t, payload, qos, retain, _ = await watcher.recv_publish(timeout=3)
    assert (t, payload) == ("will/x", b"bye")
    await watcher.disconnect()


async def test_will_not_published_on_graceful_disconnect(broker):
    watcher = broker.client()
    await watcher.connect("w", clean_session=True, username="user", password="pass")
    await watcher.subscribe([("will/g", 1)])
    polite = broker.client()
    await polite.connect("polite", clean_session=True, username="user", password="pass",
                         will_topic="will/g", will_payload=b"nope", will_qos=1)
    await polite.disconnect()
    with pytest.raises(asyncio.TimeoutError):
        await watcher.recv_publish(timeout=0.6)
    await watcher.disconnect()


async def test_retained_will(broker):
    watcher = broker.client()
    await watcher.connect("w", clean_session=True, username="user", password="pass")
    await watcher.subscribe([("status/+", 1)])
    dying = broker.client()
    await dying.connect("dying", clean_session=True, username="user", password="pass",
                        will_topic="status/dying", will_payload=b"dead",
                        will_qos=1, will_retain=True)
    await dying.hard_disconnect()
    t, p, q, retain, _ = await watcher.recv_publish(timeout=3)
    # 遗嘱正常投递给匹配订阅者：线上帧 retain=0（§3.1.2-10）。
    assert (t, p, retain) == ("status/dying", b"dead", False)
    # 但保留遗嘱已更新保留存储，可被后来的新订阅获取（retain=1）。
    later = broker.client()
    await later.connect("later", clean_session=True, username="user", password="pass")
    await later.subscribe([("status/+", 1)])
    t, p, q, retain, _ = await later.recv_publish()
    assert (t, p, retain) == ("status/dying", b"dead", True)
    await later.disconnect()
    await watcher.disconnect()


async def test_will_not_sent_on_broker_shutdown(broker):
    # 服务端主动停止不构成客户端异常断开，不应误发遗嘱。
    dying = broker.client()
    await dying.connect("dying", clean_session=False, username="user", password="pass",
                        will_topic="will/down", will_payload=b"nope", will_qos=1)
    # 重启后新订阅看不到遗嘱保留消息，也没有离线遗嘱缓存。
    restarted = await broker.restart()
    try:
        assert "will/down" not in restarted.server.broker.retained
        c = restarted.client()
        await c.connect("checker", clean_session=True, username="user", password="pass")
        await c.subscribe([("will/#", 1)])
        with pytest.raises(asyncio.TimeoutError):
            await c.recv_publish(timeout=0.5)
        await c.disconnect()
    finally:
        await restarted.stop()


async def test_will_not_published_when_broker_stops_with_online_client(inmem_broker):
    """在线持久会话带遗嘱；服务端正常停止（非崩溃）不得发布遗嘱。"""
    watcher = inmem_broker.client()
    await watcher.connect("w", clean_session=True, username="user", password="pass")
    await watcher.subscribe([("will/down", 1)])
    dying = inmem_broker.client()
    await dying.connect("online-will", clean_session=False, username="user",
                        password="pass", will_topic="will/down",
                        will_payload=b"nope", will_qos=1)
    # 不主动断开，直接停止服务（夹具外的显式 stop）。
    await inmem_broker.stop()
    with pytest.raises(asyncio.TimeoutError):
        await watcher.recv_publish(timeout=0.6)
