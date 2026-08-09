"""
Day 4 FastAPI backend.

Subscribes to all sensor topics on MQTT (sensors/#) and republishes each
reading into a Redis pub/sub channel ("sensor_readings"). This is the
decoupling point: MQTT ingestion and downstream AI processing never talk
directly to each other -- both only ever talk to Redis.

Also exposes a couple of basic HTTP endpoints for sanity-checking the
service is alive.

Run:
    uvicorn app.main:app --reload --port 8000
(from the backend/ folder, with Redis + Mosquitto running via docker compose)
"""

import json
import os
from contextlib import asynccontextmanager

import redis
import paho.mqtt.client as mqtt
from fastapi import FastAPI

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL = "sensor_readings"

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Simple in-memory counter so /health can report activity without a DB
stats = {"messages_relayed": 0}


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"[mqtt] connected to {MQTT_HOST}:{MQTT_PORT}, subscribing to sensors/#")
        client.subscribe("sensors/#")
    else:
        print(f"[mqtt] connection failed: {reason_code}")


def on_message(client, userdata, msg):
    """Every MQTT message that arrives gets pushed straight into Redis,
    unchanged. FastAPI does no processing here -- that's the AI engine's
    job, running as a completely separate process/consumer."""
    payload = msg.payload.decode()
    redis_client.publish(REDIS_CHANNEL, payload)
    stats["messages_relayed"] += 1


def make_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()  # background thread, doesn't block FastAPI's event loop
    return client


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: connect to MQTT and start the relay
    app.state.mqtt_client = make_mqtt_client()
    yield
    # Shutdown: clean disconnect
    app.state.mqtt_client.loop_stop()
    app.state.mqtt_client.disconnect()


app = FastAPI(title="Anomaly Detection Backend", lifespan=lifespan)


@app.get("/health")
def health():
    try:
        redis_ok = redis_client.ping()
    except redis.exceptions.ConnectionError:
        redis_ok = False
    return {
        "status": "ok",
        "redis_connected": redis_ok,
        "messages_relayed": stats["messages_relayed"],
    }


@app.get("/")
def root():
    return {"service": "anomaly-detection-backend", "docs": "/docs"}
