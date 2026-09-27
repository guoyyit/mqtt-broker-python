"""保活：1.5 倍宽限期内无任何报文则断开；PINGREQ 正常应答并保活。"""

from __future__ import annotations

import asyncio


async def test_keepalive_ping_roundtrip(broker):
    c = broker.client()
    await c.connect("ping-c", clean_session=True, keep_alive=60,
                    username="user", password="pass")
    await c.ping()
    await c.disconnect()


async def test_idle_connection_closed_after_grace(inmem_broker):
    # keep_alive=1s -> 1.5s 宽限后服务端断开。
    c = inmem_broker.client()
    await c.connect("idle", clean_session=True, keep_alive=1,
                    username="user", password="pass")
    # 等待读任务因 EOF 结束（不做任何 ping）。
    await asyncio.wait_for(c._reader_task, timeout=4)
    assert c._read_error is not None
