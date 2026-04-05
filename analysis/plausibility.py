from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from storage.sqlite_store import SQLiteStore
from services.time_utils import utc_now_storage


@dataclass(slots=True)
class AlertRecord:
    timestamp_utc: str
    device_name: str
    severity: str
    rule_name: str
    message: str
    context: dict[str, Any]


class PlausibilityEngine:
    def __init__(self, store: SQLiteStore, stale_seconds: int = 45) -> None:
        self.store = store
        self.stale_seconds = stale_seconds

    def run(self, device_names: list[str], current_timestamp_utc: str) -> list[AlertRecord]:
        alerts: list[AlertRecord] = []
        alerts.extend(self._check_repeated_failures(device_names, current_timestamp_utc))
        alerts.extend(self._check_stale_values(device_names, current_timestamp_utc))
        alerts.extend(self._check_missing_values(current_timestamp_utc))
        alerts.extend(self._check_grid_sign_conflicts(current_timestamp_utc))
        alerts.extend(self._check_pv_at_night(current_timestamp_utc))
        alerts.extend(self._check_wallbox_without_load(current_timestamp_utc))
        alerts.extend(self._check_unrealistic_jumps(current_timestamp_utc))
        alerts.extend(self._check_dependent_device_inconsistency(current_timestamp_utc))
        for alert in alerts:
            self.store.save_alert(
                utc_now_storage(),
                alert.device_name,
                alert.severity,
                alert.rule_name,
                alert.message,
                alert.context,
            )
        return alerts

    def _check_repeated_failures(self, device_names: list[str], timestamp_utc: str) -> list[AlertRecord]:
        alerts: list[AlertRecord] = []
        for device_name in device_names:
            events = self.store.get_recent_poll_events(device_name, limit=3)
            if len(events) == 3 and all(not event["success"] for event in events):
                alerts.append(
                    AlertRecord(
                        timestamp_utc=timestamp_utc,
                        device_name=device_name,
                        severity="high",
                        rule_name="repeated_communication_failures",
                        message=f"{device_name} failed three poll attempts in a row.",
                        context={"events": events},
                    )
                )
        return alerts

    def _check_stale_values(self, device_names: list[str], timestamp_utc: str) -> list[AlertRecord]:
        now = self._parse_ts(timestamp_utc)
        alerts: list[AlertRecord] = []
        for device_name in device_names:
            recent = self.store.get_recent_measurements("grid_power_w", device_name=device_name, limit=1)
            if not recent:
                continue
            measurement_ts = self._parse_ts(recent[0]["timestamp_utc"])
            if (now - measurement_ts) > timedelta(seconds=self.stale_seconds):
                alerts.append(
                    AlertRecord(
                        timestamp_utc=timestamp_utc,
                        device_name=device_name,
                        severity="medium",
                        rule_name="stale_values",
                        message=f"{device_name} has stale grid_power_w data older than {self.stale_seconds}s.",
                        context={"latest_measurement": recent[0]},
                    )
                )
        return alerts

    def _check_missing_values(self, timestamp_utc: str) -> list[AlertRecord]:
        required_metrics = ["grid_power_w", "house_power_w", "wallbox_power_w"]
        missing = [m for m in required_metrics if self.store.get_latest_measurement_value(m) is None]
        if not missing:
            return []
        return [
            AlertRecord(
                timestamp_utc=timestamp_utc,
                device_name="system",
                severity="medium",
                rule_name="missing_values",
                message=f"Missing latest values for: {', '.join(missing)}.",
                context={"missing_metrics": missing},
            )
        ]

    def _check_grid_sign_conflicts(self, timestamp_utc: str) -> list[AlertRecord]:
        grid = self.store.get_latest_measurement_value("grid_power_w")
        house = self.store.get_latest_measurement_value("house_power_w")
        pv = self.store.get_latest_measurement_value("pv_power_w")
        if grid is None or house is None:
            return []
        if grid < -100 and pv is None:
            return [
                AlertRecord(
                    timestamp_utc=timestamp_utc,
                    device_name="system",
                    severity="medium",
                    rule_name="suspicious_grid_power_sign",
                    message="Grid export is reported but no PV production metric is available.",
                    context={"grid_power_w": grid, "house_power_w": house, "pv_power_w": pv},
                )
            ]
        if grid > 10000 or grid < -10000:
            return [
                AlertRecord(
                    timestamp_utc=timestamp_utc,
                    device_name="system",
                    severity="medium",
                    rule_name="unrealistic_grid_power",
                    message="Grid power magnitude looks unusually high for a residential setup.",
                    context={"grid_power_w": grid},
                )
            ]
        return []

    def _check_pv_at_night(self, timestamp_utc: str) -> list[AlertRecord]:
        pv = self.store.get_latest_measurement_value("pv_power_w")
        if pv is None:
            return []
        hour = self._parse_ts(timestamp_utc).hour
        if (hour >= 22 or hour <= 5) and pv > 200:
            return [
                AlertRecord(
                    timestamp_utc=timestamp_utc,
                    device_name="system",
                    severity="low",
                    rule_name="pv_suspicious_at_night",
                    message="PV production is unexpectedly high for nighttime hours.",
                    context={"pv_power_w": pv, "hour_utc": hour},
                )
            ]
        return []

    def _check_wallbox_without_load(self, timestamp_utc: str) -> list[AlertRecord]:
        wallbox = self.store.get_latest_measurement_value("wallbox_power_w")
        house = self.store.get_latest_measurement_value("house_power_w")
        if wallbox is None or house is None:
            return []
        if wallbox > 1500 and house < wallbox * 0.4:
            return [
                AlertRecord(
                    timestamp_utc=timestamp_utc,
                    device_name="system",
                    severity="medium",
                    rule_name="wallbox_without_load_increase",
                    message="Wallbox charging is reported but house load does not increase plausibly.",
                    context={"wallbox_power_w": wallbox, "house_power_w": house},
                )
            ]
        return []

    def _check_unrealistic_jumps(self, timestamp_utc: str) -> list[AlertRecord]:
        alerts: list[AlertRecord] = []
        for metric_name in ["grid_power_w", "house_power_w", "wallbox_power_w", "pv_power_w"]:
            history = self.store.get_recent_measurements(metric_name, limit=2)
            if len(history) < 2:
                continue
            latest = history[0]["metric_value"]
            previous = history[1]["metric_value"]
            delta = abs(latest - previous)
            if delta > 12000:
                alerts.append(
                    AlertRecord(
                        timestamp_utc=timestamp_utc,
                        device_name=history[0]["device_name"],
                        severity="medium",
                        rule_name="unrealistic_jump",
                        message=f"{metric_name} changed abruptly by {delta:.0f}.",
                        context={"metric_name": metric_name, "latest": latest, "previous": previous},
                    )
                )
        return alerts

    def _check_dependent_device_inconsistency(self, timestamp_utc: str) -> list[AlertRecord]:
        alerts: list[AlertRecord] = []
        wallbox = self.store.get_latest_measurement_value("wallbox_power_w")
        pv = self.store.get_latest_measurement_value("pv_power_w")
        kostal_last = self.store.get_recent_poll_events("kostal", limit=1)
        if wallbox is not None and wallbox > 0 and (not kostal_last or not kostal_last[0]["success"]):
            alerts.append(
                AlertRecord(
                    timestamp_utc=timestamp_utc,
                    device_name="kostal",
                    severity="low",
                    rule_name="dependent_device_unavailable",
                    message="Charging data exists while inverter data source is unavailable or failing.",
                    context={"wallbox_power_w": wallbox, "kostal_last_poll": kostal_last[0] if kostal_last else None},
                )
            )
        if pv is not None and pv > 0:
            cfos_last = self.store.get_recent_poll_events("cfos", limit=1)
            if cfos_last and not cfos_last[0]["success"]:
                alerts.append(
                    AlertRecord(
                        timestamp_utc=timestamp_utc,
                        device_name="cfos",
                        severity="low",
                        rule_name="dependent_device_inconsistency",
                        message="PV data exists while cFos wallbox/controller data is currently failing.",
                        context={"pv_power_w": pv, "cfos_last_poll": cfos_last[0]},
                    )
                )
        return alerts

    def _parse_ts(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
