import time

from src.utils.messages.messageHandlerSubscriber import messageHandlerSubscriber
from src.utils.messages.allMessages import (
    FusedPose,
    LocalizationHealth,
)
from src.templates.threadwithstop import ThreadWithStop
from src.data.TrafficCommunication.traffic_comm_bridge import TrafficCommBridge


class threadTrafficCommBridge(ThreadWithStop):
    """
    Feeds TrafficCommunication shared memory using the single fused localization source.

    This thread only mirrors:
    - fused speed (speed_cm_s from FusedPose)
    - fused position (x, y) — only when healthy=True
    - fused yaw (yaw_deg) — only when healthy=True

    into TrafficCommunication shared memory. It does NOT control the car,
    run localization logic, filter UWB, or parse IMU.
    """

    def __init__(self, shared_memory, queueList, logger=None, debugging=False):
        super(threadTrafficCommBridge, self).__init__(pause=0.02)

        self.shared_memory = shared_memory
        self.queueList = queueList
        self.logger = logger
        self.debugging = bool(debugging)

        self.bridge = TrafficCommBridge(
            shared_memory=self.shared_memory,
            logger=self.logger,
            debugging=self.debugging,
        )

        self.fusedPoseSub = messageHandlerSubscriber(
            self.queueList, FusedPose, "lastOnly", True
        )
        self.localizationHealthSub = messageHandlerSubscriber(
            self.queueList, LocalizationHealth, "lastOnly", True
        )

        self._latest_pose = None
        self._latest_health = None

        self._last_flush_ts = 0.0
        self._flush_period_s = 0.10   # 10 Hz bridge update

        self._last_log_ts = 0.0
        self._log_period_s = 1.0

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def thread_work(self):
        self._update_pose_cache()
        self._update_health_cache()
        self._flush_if_due()
        self._maybe_log_status()

    # ------------------------------------------------------------------
    # Cache updates
    # ------------------------------------------------------------------
    def _update_pose_cache(self):
        msg = self.fusedPoseSub.receive()
        if not isinstance(msg, dict):
            return
        self._latest_pose = msg

    def _update_health_cache(self):
        msg = self.localizationHealthSub.receive()
        if not isinstance(msg, dict):
            return
        self._latest_health = msg

    # ------------------------------------------------------------------
    # Flush to TrafficCommunication shared memory
    # ------------------------------------------------------------------
    def _flush_if_due(self):
        now = time.time()
        if (now - self._last_flush_ts) < self._flush_period_s:
            return
        self._last_flush_ts = now

        pose = self._latest_pose
        if not isinstance(pose, dict):
            return

        speed_cm_s = self._to_float(pose.get("speed_cm_s", pose.get("cmd_speed_cm_s")))
        if speed_cm_s is not None:
            self.bridge.publish_speed_cm_s(speed_cm_s)

        x = self._to_float(pose.get("x"))
        y = self._to_float(pose.get("y"))
        yaw_deg = self._to_float(pose.get("yaw_deg"))
        healthy = bool(pose.get("healthy", False))

        # Forward position/rotation only when localization is healthy.
        if healthy and x is not None and y is not None:
            self.bridge.publish_position_m(x, y)

        if healthy and yaw_deg is not None:
            self.bridge.publish_rotation_deg(yaw_deg)

    # ------------------------------------------------------------------
    # Debug logging
    # ------------------------------------------------------------------
    def _maybe_log_status(self):
        if not self.debugging or self.logger is None:
            return

        now = time.time()
        if (now - self._last_log_ts) < self._log_period_s:
            return
        self._last_log_ts = now

        pose = self._latest_pose or {}
        health = self._latest_health or {}

        try:
            self.logger.info(
                "[TrafficCommBridge] "
                f"healthy={pose.get('healthy')} "
                f"status={health.get('status')} "
                f"x={pose.get('x')} "
                f"y={pose.get('y')} "
                f"yaw={pose.get('yaw_deg')} "
                f"speed={pose.get('speed_cm_s', pose.get('cmd_speed_cm_s'))} "
                f"uwb_q={pose.get('uwb_quality')} "
                f"uwb_ok={pose.get('last_uwb_accepted')} "
                f"nis={pose.get('last_uwb_nis')}"
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _to_float(self, value):
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None
