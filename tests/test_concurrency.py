"""并发发布/订阅/重连下的状态一致性与顺序边界。"""

from __future__ import annotations

import asyncio

import pytest


async def test_concurrent_publishers_per_connection_order(broker):
    """多个发布者并发：每个连接自身顺序保持，跨连接不承诺全局顺序。"""
    sub = broker.client()
    await sub.connect("sub", clean_session=True, username="user", password="pass")
    await sub.subscribe([("c/+", 2)])

    publishers = []
    for p in range(5):
        pub = broker.client()
        await pub.connect(f"pub{p}", clean_session=True,
                          username="user", password="pass")
        publishers.append((p, pub))

    async def burst(p, pub):
        for i in range(20):
            await pub.publish(f"c/{p}", f"{p}-{i}".encode(), qos=(i % 3))

    await asyncio.gather(*(burst(p, pub) for p, pub in publishers))
    # 收集 100 条。
    got: dict[int, list[str]] = {p: [] for p in range(5)}
    for _ in range(100):
        t, payload, *_ = await sub.recv_publish(timeout=3)
        p = int(t.split("/")[1])
        got[p].append(payload.decode())
    # 每个发布者的 20 条严格有序。
    for p in range(5):
        assert got[p] == [f"{p}-{i}" for i in range(20)], got[p][:5]
    for _, pub in publishers:
        await pub.disconnect()
    await sub.disconnect()


async def test_concurrent_subscribe_and_publish(broker):
    """订阅与发布并发：每条消息要么被某订阅确定匹配，要么确定不匹配。"""
    results = []

    async def subscriber(cid):
        c = broker.client()
        await c.connect(cid, clean_session=True, username="user", password="pass")
        codes = await c.subscribe([("shared/x", 1)])
        assert codes == [1]
        try:
            for _ in range(10):
                t, payload, *_ = await c.recv_publish(timeout=2)
                results.append((cid, payload))
        except asyncio.TimeoutError:
            pass
        await c.disconnect()

    async def publisher():
        c = broker.client()
        await c.connect("pub", clean_session=True, username="user", password="pass")
        for i in range(10):
            await c.publish("shared/x", str(i).encode(), qos=1)
        await c.disconnect()

    await asyncio.gather(subscriber("s1"), subscriber("s2"), subscriber("s3"),
                         publisher())
    # 每个在线订阅者要么收到完整 10 条，要么收到其订阅生效前的前缀，绝不重复/乱序。
    per: dict[str, list[str]] = {}
    for cid, payload in results:
        per.setdefault(cid, []).append(payload.decode())
    for cid, seq in per.items():
        expected = [str(i) for i in range(10)]
        assert seq == expected[:len(seq)], (cid, seq)


async def test_concurrent_retained_updates_converge(broker):
    """并发保留消息更新：最终值确定为其中某次完整写入，不会撕裂。"""
    async def publisher(value):
        c = broker.client()
        await c.connect(f"p{value}", clean_session=True,
                        username="user", password="pass")
        for _ in range(10):
            await c.publish("ret/key", f"v{value}".encode(), qos=1, retain=True)
        await c.disconnect()

    await asyncio.gather(*(publisher(v) for v in range(4)))
    payload, _ = broker.server.broker.retained["ret/key"]
    assert payload in {f"v{v}".encode() for v in range(4)}

    # 新订阅恰好收到一条最终保留消息。
    c = broker.client()
    await c.connect("reader", clean_session=True, username="user", password="pass")
    await c.subscribe([("ret/key", 1)])
    t, p, q, retain, _ = await c.recv_publish()
    assert retain and p == payload
    with pytest.raises(asyncio.TimeoutError):
        await c.recv_publish(timeout=0.4)
    await c.disconnect()

