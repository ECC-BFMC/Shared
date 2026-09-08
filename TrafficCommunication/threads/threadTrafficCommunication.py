# Copyright (c) 2019, Bosch Engineering Center Cluj and BFMC organizers
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.

# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.

# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
# Import necessary modules
import asyncio

from src.templates.threadwithstop import ThreadWithStop


DEFAULT_INTERVAL_SECONDS = 0.05
COMMUNICATION_SOCKET = "socket"
COMMUNICATION_UDP_TCP = "udp/tcp"


def normalize_communication_mode(mode):
    """Normalize the public communication-mode flag."""
    value = str(mode).strip().lower()
    if value == COMMUNICATION_SOCKET:
        return COMMUNICATION_SOCKET
    if value == COMMUNICATION_UDP_TCP:
        return COMMUNICATION_UDP_TCP
    raise ValueError(
        f"Unsupported traffic communication mode {mode!r}; "
        "use 'socket' or 'udp/tcp'"
    )


class threadTrafficCommunication(ThreadWithStop):
    """Thread which will handle processTrafficCommunication functionalities

    Args:
        shrd_mem (sharedMem): A space in memory for mwhere we will get and update data.
        queuesList (dictionary of multiprocessing.queues.Queue): Dictionary of queues where the ID is the type of messages.
        deviceID (int): The id of the device.
        frequency (float): Send/subscription interval in seconds.
        decrypt_key (String): A path to the decription key.
        websocket_url (String): Cloud WebSocket base URL or full socket endpoint.
        connectionType (String): "socket" or "udp/tcp".
    """

    # ====================================== INIT ==========================================
    def __init__(
        self,
        shrd_mem,
        queueslist,
        deviceID,
        frequency=DEFAULT_INTERVAL_SECONDS,
        decrypt_key=None,
        websocket_url=None,
        connectionType=COMMUNICATION_SOCKET,
    ):
        super(threadTrafficCommunication, self).__init__()
        self.queue = queueslist
        self.connectionType = normalize_communication_mode(connectionType)
        self._loop = None
        self._main_task = None
        self.websocket_client = None

        if self.connectionType == COMMUNICATION_SOCKET:
            from src.data.TrafficCommunication.threads.websocketClient import (
                WebSocketTrafficClient,
            )

            self.websocket_client = WebSocketTrafficClient(
                device_id=deviceID,
                interval_seconds=frequency,
                shared_memory=shrd_mem,
                queues=self.queue,
                websocket_url=websocket_url,
            )
        else:
            self._init_udp_tcp(shrd_mem, deviceID, frequency, decrypt_key)

    def _init_udp_tcp(self, shrd_mem, deviceID, frequency, decrypt_key):
        """Initialize the original UDP discovery and TCP communication path."""
        from twisted.internet import reactor
        from src.data.TrafficCommunication.threads.udpListener import udpListener
        from src.data.TrafficCommunication.threads.tcpClient import tcpClient
        from src.data.TrafficCommunication.useful.periodicTask import periodicTask

        self.listenPort = 9000
        self.tcp_factory = tcpClient(self.serverLost, deviceID, frequency, self.queue)
        self.udp_factory = udpListener(decrypt_key, self.serverFound)
        self.period_task = periodicTask(1, shrd_mem, self.tcp_factory)
        self.reactor = reactor
        self.reactor.listenUDP(self.listenPort, self.udp_factory)  # type: ignore

    # =================================== CONNECTION =======================================
    def serverLost(self):
        """Return the legacy transport to UDP discovery after TCP disconnects."""
        self.reactor.listenUDP(self.listenPort, self.udp_factory)  # type: ignore
        self.tcp_factory.stopListening()  # type: ignore
        self.period_task.stop()

    def serverFound(self, address, port):
        """Connect the legacy TCP client after a signed UDP broadcast."""
        self.reactor.connectTCP(address, port, self.tcp_factory)  # type: ignore
        self.udp_factory.stopListening()
        self.period_task.start()

    # ======================================= RUN ==========================================
    def thread_work(self):
        if self.connectionType == COMMUNICATION_UDP_TCP:
            self.reactor.run(installSignalHandlers=False)  # type: ignore
            return

        async def run_client():
            self._loop = asyncio.get_running_loop()
            self._main_task = asyncio.current_task()
            await self.websocket_client.run_forever()  # type: ignore

        try:
            asyncio.run(run_client())
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self.websocket_client.last_error = str(exc)
            self.websocket_client.logger.warning(
                f"Traffic WebSocket client stopped after an error: {exc}"
            )
        finally:
            self._main_task = None
            self._loop = None
            self._blocker.set()

    # ====================================== STOP ==========================================
    def stop(self):
        if self.connectionType == COMMUNICATION_UDP_TCP:
            self.reactor.callFromThread(self.reactor.stop)  # type: ignore
            super(threadTrafficCommunication, self).stop()
            return

        super(threadTrafficCommunication, self).stop()
        loop = self._loop
        task = self._main_task
        if loop is not None and task is not None and not loop.is_closed():
            loop.call_soon_threadsafe(task.cancel)
