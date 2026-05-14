import math
import time
from typing import Optional


class TrafficCommBridge:
    HISTORY_IDS = {
        "stop": 1,
        "stop_sign": 1,
        "priority": 2,
        "priority_sign": 2,
        "parking": 3,
        "parking_sign": 3,
        "crosswalk": 4,
        "crosswalk_sign": 4,
        "pedestrian_crossing": 4,
        "highway_entry": 5,
        "highway_entrance": 5,
        "highway_exit": 6,
        "highway_end": 6,
        "roundabout": 7,
        "roundabout_sign": 7,
        "one_way": 8,
        "oneway": 8,
        "no_entry": 9,
        "noentry": 9,
        "static_car_on_parking": 10,
        "pedestrian_on_crosswalk": 11,
        "pedestrian_crosswalk": 11,
        "pedestrian_on_road": 12,
        "roadblock": 13,
        "traffic_light": 14,
        "semaphore": 14,
        "fog": 15,
        "tunnel": 16,
        "ramp": 17,
    }

    def __init__(self, shared_memory, logger=None, debugging: bool = False):
        self.shared_memory = shared_memory
        self.logger = logger
        self.debugging = bool(debugging)

        self._last_speed = None
        self._last_pos = None
        self._last_rot = None

        self._history_last_sent = {}

        self.speed_epsilon = 0.5      # cm/s
        self.pos_epsilon = 0.02       # m
        self.rot_epsilon = 1.0        # deg

    # -----------------------------
    # Primary API used by the Brain
    # -----------------------------
    def publish_speed_cm_s(self, speed_cm_s: float) -> None:
        speed = float(speed_cm_s)
        if self._last_speed is not None and abs(speed - self._last_speed) < self.speed_epsilon:
            return

        self.shared_memory.insert("deviceSpeed", [speed])
        self._last_speed = speed
        self._log(f"deviceSpeed={speed:.2f}")

    def publish_position_m(self, x_m: float, y_m: float) -> None:
        x = float(x_m)
        y = float(y_m)

        if self._last_pos is not None:
            px, py = self._last_pos
            if (abs(x - px) < self.pos_epsilon) and (abs(y - py) < self.pos_epsilon):
                return

        self.shared_memory.insert("devicePos", [x, y])
        self._last_pos = (x, y)
        self._log(f"devicePos=({x:.3f}, {y:.3f})")

    def publish_rotation_deg(self, yaw_deg: float) -> None:
        yaw = self._normalize_deg(float(yaw_deg))

        if self._last_rot is not None:
            diff = abs(self._angle_diff_deg(yaw, self._last_rot))
            if diff < self.rot_epsilon:
                return

        # "deviceRot" is the BFMC server protocol key (max 12 chars fits sharedMem U12 dtype).
        self.shared_memory.insert("deviceRot", [yaw])
        self._last_rot = yaw
        self._log(f"deviceRot={yaw:.2f}")

    def publish_pose(self, x_m: float, y_m: float, yaw_deg: float) -> None:
        self.publish_position_m(x_m, y_m)
        self.publish_rotation_deg(yaw_deg)

    def publish_history_event(
        self,
        event_id: int,
        x_m: float,
        y_m: float,
        cooldown_s: float = 3.0,
        dedupe_key: Optional[str] = None,
    ) -> bool:
        event_id = int(event_id)
        x = float(x_m)
        y = float(y_m)
        now = time.time()

        if dedupe_key is None:
            dedupe_key = f"{event_id}:{round(x, 1)}:{round(y, 1)}"

        last_sent = self._history_last_sent.get(dedupe_key)
        if last_sent is not None and (now - last_sent) < float(cooldown_s):
            return False

        self.shared_memory.insert("historyData", [event_id, x, y])
        self._history_last_sent[dedupe_key] = now
        self._log(f"historyData=[{event_id}, {x:.3f}, {y:.3f}]")
        return True

    def publish_history_label(
        self,
        label: str,
        x_m: float,
        y_m: float,
        cooldown_s: float = 3.0,
        dedupe_key: Optional[str] = None,
    ) -> bool:
        event_id = self.label_to_history_id(label)
        if event_id is None:
            self._log(f"unknown history label ignored: {label}")
            return False
        return self.publish_history_event(
            event_id=event_id,
            x_m=x_m,
            y_m=y_m,
            cooldown_s=cooldown_s,
            dedupe_key=dedupe_key,
        )

    # --------------------------------
    # Helpers for localization payloads
    # --------------------------------
    def publish_from_localization(self, loc_msg: dict, yaw_deg: Optional[float] = None) -> bool:
        if not isinstance(loc_msg, dict):
            return False

        try:
            x = float(loc_msg["x"])
            y = float(loc_msg["y"])
        except Exception:
            return False

        self.publish_position_m(x, y)

        if yaw_deg is not None:
            self.publish_rotation_deg(yaw_deg)

        return True

    @classmethod
    def label_to_history_id(cls, label: str) -> Optional[int]:
        key = str(label).strip().lower().replace("-", "_").replace(" ", "_")
        return cls.HISTORY_IDS.get(key)

    # -----------------------------
    # Internal helpers
    # -----------------------------
    def _normalize_deg(self, deg: float) -> float:
        out = deg % 360.0
        if out < 0.0:
            out += 360.0
        return out

    def _angle_diff_deg(self, a: float, b: float) -> float:
        return ((a - b + 180.0) % 360.0) - 180.0

    def _log(self, msg: str) -> None:
        if not self.debugging:
            return

        if self.logger is not None:
            try:
                self.logger.info(f"[TrafficCommBridge] {msg}")
                return
            except Exception:
                pass

        print(f"[TrafficCommBridge] {msg}")
