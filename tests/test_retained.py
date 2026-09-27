"""保留消息语义。"""

from __future__ import annotations

import asyncio

import pytest


async def _clients(broker):
    sub = broker.client()
    await sub.connect("sub", clean_session=True, username="user", password="pass")
    pub = broker.client()
    await pub.connect("pub", clean_session=True, username="user", password="pass")
    return sub, pub


async def test_retained_delivered_on_new_subscribe(broker):
    sub, pub = await _clients(broker)
    try:
        await pub.publish("home/temp", b"23.5", qos=1, retain=True)
        await asyncio.sleep(0.1)
        # 发布时没有订阅者，且不应对旧订阅者产生消息。
        with pytest.raises(asyncio.TimeoutError):
            await sub.recv_publish(timeout=0.3)
        await sub.subscribe([("home/+", 1)])
        t, payload, qos, retain, _ = await sub.recv_publish()
        assert (t, payload, retain) == ("home/temp", b"23.5", True)
    finally:
        await sub.disconnect()
        await pub.disconnect()


async def test_retained_latest_wins(broker):
    sub, pub = await _clients(broker)
    try:
        await pub.publish("home/temp", b"old", qos=1, retain=True)
        await pub.publish("home/temp", b"new", qos=1, retain=True)
        await sub.subscribe([("home/temp", 1)])
        t, payload, *_ = await sub.recv_publish()
        assert payload == b"new"
    finally:
        await sub.disconnect()
        await pub.disconnect()


async def test_empty_payload_clears_retained(broker):
    sub, pub = await _clients(broker)
    try:
        await pub.publish("home/temp", b"v", qos=1, retain=True)
        # 空载荷保留消息：清除保留内容，且不投递。
        await pub.publish("home/temp", b"", qos=1, retain=True)
        await sub.subscribe([("home/#", 1)])
        with pytest.raises(asyncio.TimeoutError):
            await sub.recv_publish(timeout=0.4)
        assert "home/temp" not in broker.server.broker.retained
    finally:
        await sub.disconnect()
        await pub.disconnect()


async def test_non_retained_publish_does_not_overwrite(broker):
    sub, pub = await _clients(broker)
    try:
        await pub.publish("r/t", b"retained", qos=1, retain=True)
        await sub.subscribe([("r/t", 1)])
        assert (await sub.recv_publish())[1] == b"retained"
        # 普通（非 retain）发布不改变保留存储。
        await pub.publish("r/t", b"live", qos=1)
        assert (await sub.recv_publish())[1] == b"live"
        sub2 = broker.client()
        await sub2.connect("sub2", clean_session=True, username="user", password="pass")
        await sub2.subscribe([("r/t", 1)])
        assert (await sub2.recv_publish())[1] == b"retained"
        await sub2.disconnect()
    finally:
        await sub.disconnect()
        await pub.disconnect()


async def test_retained_not_sent_to_existing_subscriber_on_publish(broker):
    sub, pub = await _clients(broker)
    try:
        await sub.subscribe([("live/+", 2)])
        await pub.publish("live/a", b"A", qos=1, retain=True)
        # 已存在的订阅者不应在发布时收到保留消息。
        with pytest.raises(asyncio.TimeoutError):
            await sub.recv_publish(timeout=0.4)
        # 但之后新订阅者可以。
        sub2 = broker.client()
        await sub2.connect("s2", clean_session=True, username="user", password="pass")
        await sub2.subscribe([("live/a", 1)])
        assert (await sub2.recv_publish())[1] == b"A"
        await sub2.disconnect()
    finally:
        await sub.disconnect()
        await pub.disconnect()


async def test_retained_survives_restart(broker):
    pub = broker.client()
    await pub.connect("p", clean_session=True, username="user", password="pass")
    await pub.publish("keep/me", b"persisted", qos=1, retain=True)
    await pub.disconnect()
    await asyncio.sleep(0.1)
    restarted = await broker.restart()
    try:
        assert restarted.server.broker.retained["keep/me"][0] == b"persisted"
        sub = restarted.client()
        await sub.connect("s", clean_session=True, username="user", password="pass")
        await sub.subscribe([("keep/#", 1)])
        t, payload, *_ = await sub.recv_publish()
        assert payload == b"persisted"
        await sub.disconnect()
    finally:
        await restarted.stop()
