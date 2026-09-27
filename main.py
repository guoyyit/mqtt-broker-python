"""FastAPI service process hosting the MQTT broker on loopback."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from mqtt_broker.broker import Broker
from mqtt_broker.config import BrokerConfig
from mqtt_broker.server import MQTTServer


def build_broker() -> Broker:
    users: dict[str, str] = {}
    for entry in os.environ.get("MQTT_USERS", "admin:secret").split(","):
        if ":" in entry:
            name, password = entry.split(":", 1)
            users[name] = password
    config = BrokerConfig(
        host=os.environ.get("MQTT_HOST", "127.0.0.1"),
        port=int(os.environ.get("MQTT_PORT", "1883")),
        users=users,
        allow_anonymous=os.environ.get("MQTT_ALLOW_ANONYMOUS", "0") == "1",
        db_path=Path(os.environ.get("MQTT_DB_PATH", "broker_state.db")),
        audit_log_path=Path(os.environ.get("MQTT_AUDIT_LOG", "broker_audit.log")),
    )
    return Broker(config)


@asynccontextmanager
async def lifespan(app: FastAPI):
    broker = build_broker()
    server = MQTTServer(broker)
    await broker.start()
    await server.start()
    app.state.broker = broker
    app.state.mqtt_server = server
    try:
        yield
    finally:
        await server.stop()
        await broker.stop()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/mqtt/status")
def mqtt_status():
    broker: Broker = app.state.broker
    server: MQTTServer = app.state.mqtt_server
    return {
        "host": broker.config.host,
        "port": server.port,
        "allow_anonymous": broker.config.allow_anonymous,
    }
