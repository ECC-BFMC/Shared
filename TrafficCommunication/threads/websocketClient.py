"""Bidirectional cloud WebSocket transport for TrafficCommunication."""

from __future__ import annotations

import asyncio
import json
import os
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from src.utils.messages.allMessages import Location
from src.utils.messages.messageHandlerSender import messageHandlerSender

try:
    from src.utils.logConfig import get_logger
except ModuleNotFoundError:
    class _PrintLogger:
        def __getattr__(self, _):
            return print

    def get_logger(name):
        return _PrintLogger()

try:
    from websockets.asyncio.client import connect
    from websockets.exceptions import ConnectionClosed
except ImportError as exc:  # pragma: no cover - depends on the brain environment
    raise ImportError(
        "TrafficCommunication WebSocket support requires the 'websockets' package"
    ) from exc


DEFAULT_WEBSOCKET_URL = "wss://locsys.boschfuturemobility.com"
DEFAULT_INTERVAL_SECONDS = 0.05
MAX_MESSAGE_BYTES = 16 * 1024
CLOCK_SYNC_INTERVAL_SECONDS = 30.0
CLOCK_SYNC_TIMEOUT_SECONDS = 2.0


def build_websocket_uri(base_url: str, device_id: int | str) -> str:
    """Build the device-filtered socket URI from a base or endpoint URL."""
    parsed = urlsplit(base_url.strip().rstrip("/"))
    scheme = {"http": "ws", "https": "wss"}.get(parsed.scheme, parsed.scheme)
    if scheme not in {"ws", "wss"} or not parsed.netloc:
        raise ValueError("TRAFFIC_WS_URL must be an absolute ws:// or wss:// URL")

    path = parsed.path.rstrip("/")
    if not path.endswith("/devices/socket"):
        path = f"{path}/devices/socket" if path else "/devices/socket"
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["device"] = str(device_id)
    return urlunsplit((scheme, parsed.netloc, path, urlencode(query), ""))


def resolve_device_token(device_id: int | str, explicit_token: str | None) -> str:
    """Resolve a per-device token, then a shared token, then the dev default."""
    return (
        explicit_token
        or os.getenv(f"DEVICE_TOKEN_{device_id}")
        or os.getenv("TRAFFIC_DEVICE_TOKEN")
        or "dev"
    )


def normalize_location_message(message: dict, fallback_device_id: int | str) -> dict:
    """Translate a WebSocket location update to the legacy Location payload."""
    payload = message.get("payload")
    if not isinstance(payload, dict):
        payload = {}

    raw_device_id = message.get("device_id", fallback_device_id)
    try:
        device_id = int(raw_device_id)
    except (TypeError, ValueError):
        device_id = raw_device_id

    location = {
        "type": "location",
        "id": device_id,
        "x": payload.get("x", message.get("lat")),
        "y": payload.get("y", message.get("lon")),
    }
    for key in ("z", "quality"):
        value = payload.get(key, message.get(key))
        if value is not None:
            location[key] = value

    for key in ("seq", "source_seq", "device_time_ms", "event_ts_ms", "ts"):
        if message.get(key) is not None:
            location[key] = message[key]

    server_timestamp = message.get("server_ts")
    event_timestamp = message.get("event_ts_ms")
    try:
        if server_timestamp is not None and event_timestamp is not None:
            location["source_age_seconds"] = max(
                0.0,
                (float(server_timestamp) - float(event_timestamp)) / 1000.0,
            )
    except (TypeError, ValueError):
        pass
    return location


class WebSocketTrafficClient:
    """Drain client observations and receive location data on one socket."""

    def __init__(
        self,
        device_id,
        interval_seconds,
        shared_memory,
        queues,
        websocket_url=None,
        device_token=None,
    ):
        interval_seconds = float(interval_seconds)
        if not 0.05 <= interval_seconds <= 5.0:
            raise ValueError(
                "Traffic communication interval must be between 0.05 and 5 seconds"
            )

        self.device_id = device_id
        self.interval_seconds = interval_seconds
        self.shared_memory = shared_memory
        self.uri = build_websocket_uri(
            websocket_url or os.getenv("TRAFFIC_WS_URL", DEFAULT_WEBSOCKET_URL),
            device_id,
        )
        self.device_token = resolve_device_token(device_id, device_token)
        self.logger = get_logger("Traffic Communication")
        self.send_location = messageHandlerSender(queues, Location)
        self.websocket = None
        self.connection_count = 0
        self.last_error = None
        self.clock_offset_ms = 0.0
        self.clock_round_trip_ms = None
        self.clock_synchronized = False
        self._clock_sync_event = asyncio.Event()

    async def run_forever(self) -> None:
        headers = {"Authorization": f"Bearer {self.device_token}"}
        async for websocket in connect(
            self.uri,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=20,
            max_size=MAX_MESSAGE_BYTES,
            compression=None,
        ):
            self.websocket = websocket
            self.connection_count += 1
            self.last_error = None
            self.logger.info(
                f"Connected device {self.device_id} to WebSocket server {self.uri}"
            )
            try:
                self.clock_synchronized = False
                self._clock_sync_event.clear()
                await self._subscribe(websocket)
                receiver = asyncio.create_task(self._receive_messages(websocket))
                sender = None
                try:
                    await self._request_clock_sync(websocket)
                    try:
                        await asyncio.wait_for(
                            self._clock_sync_event.wait(),
                            timeout=CLOCK_SYNC_TIMEOUT_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        self.logger.warning(
                            "Traffic WebSocket clock synchronization timed out"
                        )
                    sender = asyncio.create_task(self._send_observations(websocket))
                    await asyncio.gather(sender, receiver)
                finally:
                    if sender is not None:
                        sender.cancel()
                    receiver.cancel()
                    tasks = [receiver] if sender is None else [sender, receiver]
                    await asyncio.gather(*tasks, return_exceptions=True)
            except ConnectionClosed as exc:
                self.last_error = str(exc)
                self.logger.warning(
                    f"WebSocket connection lost for device {self.device_id}: {exc}"
                )
                await asyncio.sleep(0.25)
            finally:
                self.websocket = None

    async def close(self) -> None:
        if self.websocket is not None:
            await self.websocket.close(code=1000, reason="client stopping")

    async def _subscribe(self, websocket) -> None:
        await self._send_json(
            websocket,
            {
                "reqORinfo": "info",
                "type": "locIDsub",
                "locID": int(self.device_id),
                "freq": self.interval_seconds,
            },
        )

    async def _send_observations(self, websocket) -> None:
        next_send = time.monotonic()
        next_clock_sync = next_send + CLOCK_SYNC_INTERVAL_SECONDS
        while True:
            for message in self.shared_memory.get():
                await self._send_json(
                    websocket,
                    {
                        **message,
                        "sent_ts_ms": int(time.time() * 1000 + self.clock_offset_ms),
                        "clock_synchronized": self.clock_synchronized,
                        "clock_sync_rtt_ms": self.clock_round_trip_ms,
                    },
                )
            if time.monotonic() >= next_clock_sync:
                await self._request_clock_sync(websocket)
                next_clock_sync = time.monotonic() + CLOCK_SYNC_INTERVAL_SECONDS
            next_send += self.interval_seconds
            await asyncio.sleep(max(0.0, next_send - time.monotonic()))

    async def _request_clock_sync(self, websocket) -> None:
        await self._send_json(
            websocket,
            {
                "reqORinfo": "request",
                "type": "clockSync",
                "client_send_ts_ms": int(time.time() * 1000),
            },
        )

    async def _receive_messages(self, websocket) -> None:
        async for raw_message in websocket:
            try:
                message = json.loads(raw_message)
            except (TypeError, json.JSONDecodeError) as exc:
                self.logger.warning(f"Ignoring malformed WebSocket message: {exc}")
                continue
            if not isinstance(message, dict):
                self.logger.warning("Ignoring non-object WebSocket message")
                continue

            message_type = message.get("type")
            if message_type == "snapshot":
                devices = message.get("devices", {})
                if isinstance(devices, dict):
                    for location in devices.values():
                        if isinstance(location, dict):
                            self._forward_location(location)
            elif message_type == "location-update":
                self._forward_location(message)
            elif message_type == "clock-sync":
                self._update_clock_offset(message, time.time() * 1000)
            elif message_type == "error":
                self.logger.warning(
                    "Traffic server rejected a message: "
                    f"{message.get('error', 'unknown error')}"
                )
            elif message_type == "hello":
                self.logger.info(
                    "Traffic WebSocket ready "
                    f"(persist_observations={message.get('persist_observations')})"
                )

    def _update_clock_offset(self, message: dict, client_receive_ts_ms: float) -> None:
        try:
            client_send = float(message["client_send_ts_ms"])
            server_receive = float(message["server_receive_ts_ms"])
            server_send = float(message["server_send_ts_ms"])
        except (KeyError, TypeError, ValueError):
            self.logger.warning("Ignoring malformed WebSocket clock-sync response")
            return

        round_trip = (client_receive_ts_ms - client_send) - (
            server_send - server_receive
        )
        self.clock_offset_ms = (
            (server_receive - client_send) + (server_send - client_receive_ts_ms)
        ) / 2.0
        self.clock_round_trip_ms = max(0.0, round_trip)
        self.clock_synchronized = True
        self._clock_sync_event.set()

    def _forward_location(self, message: dict) -> None:
        location = normalize_location_message(message, self.device_id)
        if location.get("x") is None or location.get("y") is None:
            return
        self.send_location.send(location)

    @staticmethod
    async def _send_json(websocket, message: dict) -> None:
        await websocket.send(json.dumps(message, separators=(",", ":")))
