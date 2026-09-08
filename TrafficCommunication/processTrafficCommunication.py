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

if __name__ == "__main__":
    import sys
    from pathlib import Path

    project_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(project_root))

# Import necessary modules
from multiprocessing import Pipe
from src.data.TrafficCommunication.useful.sharedMem import sharedMem
from src.templates.workerprocess import WorkerProcess
from src.data.TrafficCommunication.threads.threadTrafficCommunication import threadTrafficCommunication
try:
    from src.utils.logConfig import get_logger
except ModuleNotFoundError:
    class _PrintLogger:
        def __getattr__(self, _):
            return print
    def get_logger(name):
        return _PrintLogger()

class processTrafficCommunication(WorkerProcess):
    """This process receives the location of the car and sends it to the processGateway.
    
    Args:
        queueList (dictionary of multiprocessing.queues.Queue): Dictionary of queues where the ID is the type of messages.
        deviceID (int): The ID of the device.
        frequency (float): The frequency of communication.
        connectionType (String): "socket" or "udp/tcp".
    """

    # ====================================== INIT ==========================================
    def __init__(self, queueList, deviceID, ready_event=None, debugging=False,
                 frequency=0.05, connectionType="socket"):
        self.queuesList = queueList
        self.shared_memory = sharedMem()
        self.filename = "src/data/TrafficCommunication/useful/publickey_server_test.pem"
        self.deviceID = deviceID
        self.frequency = frequency
        self.websocket_url = "wss://locsys.boschfuturemobility.com"
        if connectionType not in {"socket", "udp/tcp"}:
            raise ValueError("connectionType must be 'socket' or 'udp/tcp'")
        self.connectionType = connectionType
        self.debugging = debugging
        super(processTrafficCommunication, self).__init__(self.queuesList, ready_event)

    # ===================================== INIT TH ======================================
    def _init_threads(self):
        """Create the Traffic Communication thread and add it to the list of threads."""

        TrafficComTh = threadTrafficCommunication(
            self.shared_memory, self.queuesList, self.deviceID, self.frequency,
            self.filename, self.websocket_url, self.connectionType
        )
        self.threads.append(TrafficComTh)


# =================================== EXAMPLE =========================================
#             ++    THIS WILL RUN ONLY IF YOU RUN THE CODE FROM HERE  ++
#                  in terminal:    python3 processTrafficCommunication.py
#                  on Windows:     python processTrafficCommunication.py

if __name__ == "__main__":
    import argparse
    import math
    import random
    import time
    from multiprocessing import Queue
    from queue import Empty

    parser = argparse.ArgumentParser(
        description="Run the brain TrafficCommunication client locally"
    )
    parser.add_argument("--device", type=int, default=99)
    parser.add_argument("--interval", type=float, default=0.05)
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Seconds to run; 0 runs until Ctrl+C",
    )
    args = parser.parse_args()
    if not 0.05 <= args.interval <= 5.0:
        parser.error("--interval must be between 0.05 and 5 seconds")
    if args.duration < 0:
        parser.error("--duration cannot be negative")

    shared_memory = sharedMem()
    locsysReceivePipe, locsysSendPipe = Pipe(duplex=False)
    queueList = {
        "Critical": Queue(),
        "Warning": Queue(),
        "General": Queue(),
        "Config": Queue(),
    }
    # filename = "useful/publickey_server.pem"
    filename = "useful/publickey_server_test.pem"
    connectionType = "socket"  # Use "udp/tcp" for the old mode.
    websocket_url = "wss://locsys.boschfuturemobility.com"
    if connectionType == "udp/tcp":
        filename = str(
            Path(__file__).resolve().parent / "useful" / "publickey_server_test.pem"
        )
    deviceID = args.device
    frequency = args.interval
    traffic_communication = threadTrafficCommunication(
        shared_memory, queueList, deviceID, frequency, filename, websocket_url,
        connectionType
    )

    print("LocSys brain TrafficCommunication local runner")
    print(f"  device:   {deviceID}")
    print(f"  mode:      {connectionType}")
    if connectionType == "socket":
        print(f"  endpoint: {traffic_communication.websocket_client.uri}")
    else:
        print("  endpoint: UDP discovery on port 9000, followed by TCP")
    print(
        f"  interval: {frequency:g}s "
        f"({1.0 / frequency:g} Hz telemetry cycles)"
    )
    print("  stop:     Ctrl+C")

    traffic_communication.start()
    start_time = time.monotonic()
    next_cycle = start_time
    next_report = start_time + 1.0
    duration = args.duration
    cycle = 0
    received_locations = 0
    latest_location = None
    failed = False
    x = 9.0
    y = 15.0
    rotation = 0.0
    speed = 8.1

    try:
        while duration == 0 or time.monotonic() - start_time < duration:
            now = time.monotonic()
            if not traffic_communication.is_alive():
                transport_error = "unknown error"
                if traffic_communication.websocket_client is not None:
                    transport_error = (
                        traffic_communication.websocket_client.last_error
                        or transport_error
                    )
                print(
                    "[traffic] communication thread stopped unexpectedly: "
                    f"{transport_error}"
                )
                failed = True
                break
            if now >= next_cycle:
                cycle += 1
                x += random.uniform(-0.05, 0.05)
                y += random.uniform(-0.05, 0.05)
                rotation = (rotation + 5.0) % 360.0
                speed = max(0.0, speed + random.uniform(-0.2, 0.2))
                shared_memory.insert("devicePos", [x, y])
                shared_memory.insert("deviceRot", [rotation])
                shared_memory.insert("deviceSpeed", [speed])
                if cycle % max(1, math.ceil(5.0 / frequency)) == 0:
                    shared_memory.insert("historyData", [1, x + 0.5, y + 0.5])
                next_cycle += frequency

            while True:
                try:
                    envelope = queueList["General"].get_nowait()
                except Empty:
                    break
                latest_location = envelope.get("msgValue", envelope)
                received_locations += 1

            if now >= next_report:
                if traffic_communication.websocket_client is not None:
                    connected = traffic_communication.websocket_client.websocket is not None
                else:
                    connected = traffic_communication.tcp_factory.connection is not None
                location_text = "waiting"
                if latest_location is not None:
                    location_text = (
                        f"id={latest_location.get('id')} "
                        f"x={latest_location.get('x')} "
                        f"y={latest_location.get('y')}"
                    )
                print(
                    f"[traffic] connected={'yes' if connected else 'no'} "
                    f"cycles={cycle} "
                    f"locations={received_locations} latest=[{location_text}]"
                )
                next_report += 1.0

            time.sleep(min(0.01, max(0.0, next_cycle - time.monotonic())))
    except KeyboardInterrupt:
        print("\n[traffic] stopping")
    finally:
        traffic_communication.stop()
        traffic_communication.join(timeout=5)
        if traffic_communication.is_alive():
            print("[traffic] warning: communication thread did not stop within 5 seconds")
        else:
            print("[traffic] stopped")
    if (
        duration > 0
        and traffic_communication.websocket_client is not None
        and traffic_communication.websocket_client.connection_count == 0
    ):
        print("[traffic] error: no WebSocket connection was established")
        failed = True
    raise SystemExit(1 if failed else 0)
