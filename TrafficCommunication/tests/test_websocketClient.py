import asyncio
import os
import json
import queue
import unittest
from unittest.mock import patch

from src.data.TrafficCommunication.threads.websocketClient import (
    WebSocketTrafficClient,
    build_websocket_uri,
    normalize_location_message,
    resolve_device_token,
)


class FakeSharedMemory:
    def get(self):
        return []


class FakeWebSocket:
    def __init__(self, received=None):
        self.received = list(received or [])
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.received:
            raise StopAsyncIteration
        return self.received.pop(0)


class FakeLocationSender:
    def __init__(self):
        self.messages = []

    def send(self, message):
        self.messages.append(message)


class WebSocketClientTests(unittest.TestCase):
    def test_builds_device_filtered_websocket_uri(self):
        self.assertEqual(
            "wss://locsys.boschfuturemobility.com/devices/socket?device=99",
            build_websocket_uri("https://locsys.boschfuturemobility.com/", 99),
        )
        self.assertEqual(
            "ws://localhost:5000/devices/socket?mode=test&device=5",
            build_websocket_uri(
                "ws://localhost:5000/devices/socket?mode=test",
                5,
            ),
        )

    def test_rejects_non_websocket_base_url(self):
        with self.assertRaises(ValueError):
            build_websocket_uri("locsys.boschfuturemobility.com", 99)

    def test_normalizes_location_for_existing_brain_consumers(self):
        normalized = normalize_location_message(
            {
                "type": "location-update",
                "device_id": "99",
                "lat": 9.2,
                "lon": 15.1,
                "seq": 42,
                "event_ts_ms": 1_000,
                "server_ts": 1_075,
                "payload": {
                    "x": 9.25,
                    "y": 15.15,
                    "z": 0,
                    "quality": 100,
                },
            },
            5,
        )

        self.assertEqual("location", normalized["type"])
        self.assertEqual(99, normalized["id"])
        self.assertEqual(9.25, normalized["x"])
        self.assertEqual(15.15, normalized["y"])
        self.assertEqual(0, normalized["z"])
        self.assertEqual(100, normalized["quality"])
        self.assertEqual(42, normalized["seq"])
        self.assertEqual(0.075, normalized["source_age_seconds"])

    def test_token_resolution_prefers_device_specific_token(self):
        with patch.dict(
            os.environ,
            {
                "DEVICE_TOKEN_99": "device-secret",
                "TRAFFIC_DEVICE_TOKEN": "shared-secret",
            },
            clear=True,
        ):
            self.assertEqual("device-secret", resolve_device_token(99, None))
            self.assertEqual("explicit", resolve_device_token(99, "explicit"))


class WebSocketClientAsyncTests(unittest.IsolatedAsyncioTestCase):
    def make_client(self):
        return WebSocketTrafficClient(
            device_id=99,
            interval_seconds=0.05,
            shared_memory=FakeSharedMemory(),
            queues={"General": queue.Queue()},
            websocket_url="ws://localhost:5000",
            device_token="dev",
        )

    async def test_subscription_uses_configured_device_and_interval(self):
        client = self.make_client()
        websocket = FakeWebSocket()

        await client._subscribe(websocket)

        self.assertEqual(
            {
                "reqORinfo": "info",
                "type": "locIDsub",
                "locID": 99,
                "freq": 0.05,
            },
            json.loads(websocket.sent[0]),
        )

    async def test_receiver_forwards_snapshot_and_live_location(self):
        client = self.make_client()
        sender = FakeLocationSender()
        client.send_location = sender
        websocket = FakeWebSocket(
            [
                json.dumps(
                    {
                        "type": "snapshot",
                        "devices": {
                            "99": {
                                "device_id": "99",
                                "lat": 9.2,
                                "lon": 15.1,
                                "payload": {"x": 9.2, "y": 15.1},
                            }
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "location-update",
                        "device_id": "99",
                        "lat": 9.3,
                        "lon": 15.2,
                        "payload": {"x": 9.3, "y": 15.2, "quality": 100},
                    }
                ),
            ]
        )

        await client._receive_messages(websocket)

        self.assertEqual(2, len(sender.messages))
        self.assertEqual(99, sender.messages[0]["id"])
        self.assertEqual(9.2, sender.messages[0]["x"])
        self.assertEqual(15.2, sender.messages[1]["y"])
        self.assertEqual(100, sender.messages[1]["quality"])

    async def test_observations_include_send_timestamp(self):
        class OneMessageSharedMemory:
            def __init__(self):
                self.pending = [{"reqORinfo": "info", "type": "deviceSpeed", "value1": 8.1}]

            def get(self):
                messages, self.pending = self.pending, []
                return messages

        client = self.make_client()
        client.shared_memory = OneMessageSharedMemory()
        client.clock_offset_ms = 50.0
        client.clock_round_trip_ms = 4.0
        client.clock_synchronized = True
        websocket = FakeWebSocket()
        with patch(
            "src.data.TrafficCommunication.threads.websocketClient.time.time",
            return_value=1.0,
        ):
            sender = asyncio.create_task(client._send_observations(websocket))
            try:
                while not websocket.sent:
                    await asyncio.sleep(0)
            finally:
                sender.cancel()
                await asyncio.gather(sender, return_exceptions=True)

        observation = json.loads(websocket.sent[0])
        self.assertEqual("deviceSpeed", observation["type"])
        self.assertEqual(1_050, observation["sent_ts_ms"])
        self.assertTrue(observation["clock_synchronized"])
        self.assertEqual(4.0, observation["clock_sync_rtt_ms"])

    def test_clock_sync_compensates_for_clock_offset(self):
        client = self.make_client()
        client._update_clock_offset(
            {
                "client_send_ts_ms": 1_000,
                "server_receive_ts_ms": 1_070,
                "server_send_ts_ms": 1_072,
            },
            client_receive_ts_ms=1_042,
        )

        self.assertTrue(client.clock_synchronized)
        self.assertEqual(50.0, client.clock_offset_ms)
        self.assertEqual(40.0, client.clock_round_trip_ms)


if __name__ == "__main__":
    unittest.main()
