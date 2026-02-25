from __future__ import annotations

import asyncio
import json
from typing import Any

import redis.asyncio as redis
import structlog
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

log = structlog.get_logger()


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []
        self.redis: redis.Redis | None = None
        self.pubsub: Any = None
        self.task: asyncio.Task | None = None
        self.broadcast_attempt_total = 0
        self.broadcast_success_total = 0
        self.broadcast_failures_total = 0
        self.dead_clients_pruned_total = 0
        self.last_broadcast_error = ""

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)
        if not self.redis:
            await self._start_redis()

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str) -> None:
        if not self.active_connections:
            return
        alive_connections: list[WebSocket] = []
        failures_this_round = 0
        for connection in list(self.active_connections):
            self.broadcast_attempt_total = int(self.broadcast_attempt_total) + 1
            try:
                await connection.send_text(message)
                self.broadcast_success_total = int(self.broadcast_success_total) + 1
                alive_connections.append(connection)
            except Exception as e:
                failures_this_round += 1
                self.broadcast_failures_total = int(self.broadcast_failures_total) + 1
                self.last_broadcast_error = str(e)
        pruned = int(len(self.active_connections) - len(alive_connections))
        if pruned > 0:
            self.active_connections = alive_connections
            self.dead_clients_pruned_total = int(self.dead_clients_pruned_total) + int(pruned)
            log.warning(
                "websocket.dead_clients_pruned",
                pruned=int(pruned),
                active_connections=int(len(self.active_connections)),
                failures_this_round=int(failures_this_round),
                failures_total=int(self.broadcast_failures_total),
            )
        elif failures_this_round == 0:
            self.last_broadcast_error = ""

    async def _start_redis(self) -> None:
        try:
            self.redis = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
            self.pubsub = self.redis.pubsub()
            await self.pubsub.psubscribe("ghtrader:*:updates:*")
            self.task = asyncio.create_task(self._redis_listener())
        except Exception as e:
            log.warning("websocket.redis_connect_failed", error=str(e))

    async def _redis_listener(self) -> None:
        if not self.pubsub:
            return
        try:
            async for message in self.pubsub.listen():
                if message["type"] == "pmessage":
                    channel = message["channel"]
                    data = message["data"]
                    try:
                        payload = json.loads(data)
                    except Exception:
                        payload = data
                    await self.broadcast(json.dumps({"channel": channel, "data": payload}, default=str))
        except Exception as e:
            log.warning("websocket.redis_listener_error", error=str(e))


def mount_dashboard_websocket(app: FastAPI, *, path: str = "/ws/dashboard") -> ConnectionManager:
    manager = ConnectionManager()
    app.state.websocket_manager = manager

    @app.websocket(path)
    async def websocket_endpoint(websocket: WebSocket) -> None:
        await manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(websocket)

    return manager

