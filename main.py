"""服务进程入口：同时提供 HTTP 状态接口与 MQTT 3.1.1 本地消息代理。

MQTT broker 在 FastAPI lifespan 中随进程启动/停止；
纯进程内运行，不依赖任何外部服务。
"""

from __future__ import annotations

import asyncio
import contextlib
import os

from fastapi import FastAPI

from mqtt_broker.config import BrokerConfig, load_config
from mqtt_broker.server import MQTTServer

# 默认演示账号，可通过环境变量覆盖。
DEFAULT_CREDENTIALS = {"admin": "public"}


def build_config() -> BrokerConfig:
    config = load_config()
    if not config.credentials:
        config.credentials = dict(DEFAULT_CREDENTIALS)
    data_dir = os.environ.get("MQTT_DATA_DIR")
    if data_dir:
        config.data_dir = data_dir
    return config


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    config = build_config()
    server = MQTTServer(config)
    app.state.mqtt_server = server
    await server.start()
    task = asyncio.create_task(server.serve_forever())
    app.state.mqtt_task = task
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await server.stop()


app = FastAPI(title="Local MQTT 3.1.1 Broker", lifespan=lifespan)


@app.get("/")
def read_root():
    return {"service": "mqtt-broker", "protocol": "MQTT 3.1.1"}


@app.get("/health")
def health():
    server: MQTTServer | None = getattr(app.state, "mqtt_server", None)
    if server is None:
        return {"status": "starting"}
    broker = server.broker
    return {
        "status": "running",
        "listening": f"{server.config.host}:{server.config.port}",
        "online_connections": len(broker.connections),
        "sessions": len(broker.sessions),
        "retained_topics": len(broker.retained),
    }
