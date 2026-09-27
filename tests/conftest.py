"""pytest 夹具：本地回环 broker，每个用例独立端口与独立持久化目录。"""

from __future__ import annotations

import asyncio
import socket
import contextlib

import pytest

from mqtt_broker.config import BrokerConfig
from mqtt_broker.server import MQTTServer
from mqtt_broker.testclient import MQTTTestClient


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class BrokerHarness:
    def __init__(self, server: MQTTServer, port: int, data_dir) -> None:
        self.server = server
        self.port = port
        self.data_dir = data_dir

    def client(self) -> MQTTTestClient:
        return MQTTTestClient("127.0.0.1", self.port)

    async def stop(self) -> None:
        await self.server.stop()

    async def crash(self) -> None:
        """模拟崩溃：不做优雅关闭（不写快照、不发遗嘱）。"""
        self.server._server.close()
        abort = getattr(self.server._server, "abort_clients", None)
        if abort is not None:
            abort()
        await self.server._server.wait_closed()
        for task in list(self.server._tasks):
            task.cancel()
        if self.server._tasks:
            await asyncio.gather(*self.server._tasks, return_exceptions=True)
        # 直接关闭 WAL（无快照），并标记 broker 已停止，避免夹具二次停止。
        if self.server.broker.store is not None:
            self.server.broker.store.close()
            self.server.broker.store = None
        self.server.broker._stopping = True

    async def restart(self, *, credentials=None) -> "BrokerHarness":
        await self.crash()
        return await start_broker(self.data_dir, port=_free_port(),
                                  credentials=credentials)


async def start_broker(data_dir, *, port: int, persistent: bool = True,
                       credentials=None, fsync: bool = True) -> BrokerHarness:
    cfg = BrokerConfig(
        host="127.0.0.1", port=port,
        credentials={"user": "pass"} if credentials is None else dict(credentials),
        data_dir=str(data_dir) if persistent else None,
        fsync=fsync,
    )
    server = MQTTServer(cfg)
    await server.start()
    return BrokerHarness(server, port, data_dir)


@pytest.fixture
async def broker(tmp_path):
    harness = await start_broker(tmp_path / "data", port=_free_port())
    try:
        yield harness
    finally:
        with contextlib.suppress(Exception):
            await harness.stop()


@pytest.fixture
async def inmem_broker(tmp_path):
    harness = await start_broker(tmp_path, port=_free_port(), persistent=False)
    try:
        yield harness
    finally:
        with contextlib.suppress(Exception):
            await harness.stop()
