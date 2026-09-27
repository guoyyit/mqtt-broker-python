from __future__ import annotations

import asyncio
import threading
import tempfile
from pathlib import Path

import pytest

from mqtt_broker.broker import Broker
from mqtt_broker.config import BrokerConfig
from mqtt_broker.server import MQTTServer


class BrokerFixture:
    """Runs a Broker + MQTTServer on a private loop in a background thread."""

    def __init__(self, config: BrokerConfig) -> None:
        self.config = config
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.broker: Broker
        self.server: MQTTServer
        self._thread.start()
        assert self._ready.wait(timeout=10)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self.broker = Broker(self.config)
        self.server = MQTTServer(self.broker)
        self._loop.run_until_complete(self.broker.start())
        self._loop.run_until_complete(self.server.start())
        self.port = self.server.port
        self._ready.set()
        self._loop.run_forever()
        self._loop.run_until_complete(self.server.stop())
        self._loop.run_until_complete(self.broker.stop())
        self.broker.close()

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)


@pytest.fixture()
def broker_factory(tmp_path):
    """Factory: each call starts a broker; all are stopped at test end."""
    started: list[BrokerFixture] = []
    counter = {"n": 0}

    def make(**overrides) -> BrokerFixture:
        counter["n"] += 1
        n = counter["n"]
        cfg = BrokerConfig(
            host="127.0.0.1",
            port=0,
            users=overrides.pop("users", {"user": "pass"}),
            allow_anonymous=overrides.pop("allow_anonymous", False),
            db_path=overrides.pop("db_path", tmp_path / f"broker{n}.db"),
            audit_log_path=overrides.pop("audit_log_path", tmp_path / f"audit{n}.log"),
        )
        fx = BrokerFixture(cfg)
        started.append(fx)
        return fx

    yield make
    for fx in started:
        fx.stop()


@pytest.fixture()
def broker(broker_factory):
    return broker_factory()
