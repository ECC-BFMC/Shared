import queue
import unittest
from unittest.mock import patch

from src.data.TrafficCommunication.threads.threadTrafficCommunication import (
    COMMUNICATION_SOCKET,
    COMMUNICATION_UDP_TCP,
    threadTrafficCommunication,
)
from src.data.TrafficCommunication.processTrafficCommunication import (
    processTrafficCommunication,
)


class FakeSharedMemory:
    def get(self):
        return []


class TrafficCommunicationTransportTests(unittest.TestCase):
    def setUp(self):
        self.queues = {"General": queue.Queue()}

    def test_websocket_is_the_default_transport(self):
        thread = threadTrafficCommunication(
            FakeSharedMemory(),
            self.queues,
            99,
            websocket_url="ws://localhost:5000",
        )

        self.assertEqual(COMMUNICATION_SOCKET, thread.connectionType)
        self.assertIsNotNone(thread.websocket_client)

    def test_udp_tcp_transport_uses_legacy_initializer(self):
        shared_memory = FakeSharedMemory()
        with patch.object(threadTrafficCommunication, "_init_udp_tcp") as initializer:
            thread = threadTrafficCommunication(
                shared_memory,
                self.queues,
                99,
                decrypt_key="publickey.pem",
                connectionType="udp/tcp",
            )

        self.assertEqual(COMMUNICATION_UDP_TCP, thread.connectionType)
        self.assertIsNone(thread.websocket_client)
        initializer.assert_called_once_with(
            shared_memory,
            99,
            0.05,
            "publickey.pem",
        )

    def test_rejects_unknown_transport(self):
        for mode in ("unknown", "legacy", "udp-tcp", "websocket"):
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                threadTrafficCommunication(
                    FakeSharedMemory(),
                    self.queues,
                    99,
                    connectionType=mode,
                )

    def test_process_passes_legacy_mode_to_communication_thread(self):
        process = processTrafficCommunication(
            self.queues,
            99,
            connectionType="udp/tcp",
        )

        with patch(
            "src.data.TrafficCommunication.processTrafficCommunication.threadTrafficCommunication"
        ) as thread_factory:
            process._init_threads()

        self.assertEqual(COMMUNICATION_UDP_TCP, process.connectionType)
        self.assertEqual(COMMUNICATION_UDP_TCP, thread_factory.call_args.args[-1])


if __name__ == "__main__":
    unittest.main()
