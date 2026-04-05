from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.time_utils import normalize_storage_timestamp, parse_utc_timestamp, utc_now_storage


@dataclass(slots=True)
class RuntimeTimingSettings:
    analytics_refresh_interval_seconds: int = 30
    poll_interval_seconds: int = 10
    raw_write_interval_seconds: int = 10
    derived_write_interval_seconds: int = 10
    rollup_interval_seconds: int = 60
    retention_days_raw: int = 30
    retention_days_rollup: int = 180
    persistence_enabled: bool = True
    live_refresh_enabled: bool = True
    cleanup_enabled: bool = True


class RuntimeTimingCoordinator:
    def __init__(self, settings: RuntimeTimingSettings, baseline: dict[str, Any] | None = None) -> None:
        self.settings = settings
        baseline = baseline or {}
        self.last_successful_poll_utc = baseline.get("last_successful_poll_utc")
        self.last_successful_raw_write_utc = baseline.get("last_successful_raw_write_utc")
        self.last_successful_derived_write_utc = baseline.get("last_successful_derived_write_utc")
        self.last_successful_rollup_utc = baseline.get("last_successful_rollup_utc")
        self.last_successful_kpi_summary_utc = baseline.get("last_successful_kpi_summary_utc")
        self.last_successful_cleanup_utc = baseline.get("last_successful_cleanup_utc")
        self.last_poll_error = baseline.get("last_poll_error")
        self.last_raw_write_error = baseline.get("last_raw_write_error")
        self.last_derived_write_error = baseline.get("last_derived_write_error")
        self.last_rollup_error = baseline.get("last_rollup_error")
        self.last_cleanup_error = baseline.get("last_cleanup_error")
        self.last_plan_utc: str | None = None
        self.last_plan: dict[str, Any] = {
            "raw_write_due": False,
            "derived_write_due": False,
            "rollup_due": False,
            "kpi_due": False,
            "cleanup_due": False,
            "persistence_enabled": settings.persistence_enabled,
        }

    def update_settings(self, settings: RuntimeTimingSettings) -> None:
        self.settings = settings

    def plan_cycle(self, timestamp_utc: str | None = None) -> dict[str, Any]:
        planned_at_utc = normalize_storage_timestamp(timestamp_utc) or timestamp_utc or utc_now_storage()
        persistence_enabled = self.settings.persistence_enabled
        plan = {
            "planned_at_utc": planned_at_utc,
            "raw_write_due": persistence_enabled and self._is_due(self.last_successful_raw_write_utc, self.settings.raw_write_interval_seconds, planned_at_utc),
            "derived_write_due": persistence_enabled and self._is_due(self.last_successful_derived_write_utc, self.settings.derived_write_interval_seconds, planned_at_utc),
            "rollup_due": persistence_enabled and self._is_due(self.last_successful_rollup_utc, self.settings.rollup_interval_seconds, planned_at_utc),
            "kpi_due": persistence_enabled and self._is_due(self.last_successful_kpi_summary_utc, self.settings.rollup_interval_seconds, planned_at_utc),
            "cleanup_due": persistence_enabled
            and self.settings.cleanup_enabled
            and self._is_due(self.last_successful_cleanup_utc, 3600, planned_at_utc),
            "persistence_enabled": persistence_enabled,
        }
        self.last_plan_utc = planned_at_utc
        self.last_plan = plan
        return plan

    def mark_poll(self, timestamp_utc: str, *, success: bool, error: str | None = None) -> None:
        if success:
            self.last_successful_poll_utc = normalize_storage_timestamp(timestamp_utc) or timestamp_utc
            self.last_poll_error = None
        elif error:
            self.last_poll_error = error

    def mark_raw_write(self, timestamp_utc: str, *, success: bool, error: str | None = None) -> None:
        if success:
            self.last_successful_raw_write_utc = normalize_storage_timestamp(timestamp_utc) or timestamp_utc
            self.last_raw_write_error = None
        elif error:
            self.last_raw_write_error = error

    def mark_derived_write(self, timestamp_utc: str, *, success: bool, error: str | None = None) -> None:
        if success:
            self.last_successful_derived_write_utc = normalize_storage_timestamp(timestamp_utc) or timestamp_utc
            self.last_derived_write_error = None
        elif error:
            self.last_derived_write_error = error

    def mark_rollup(self, timestamp_utc: str, *, success: bool, error: str | None = None) -> None:
        if success:
            normalized = normalize_storage_timestamp(timestamp_utc) or timestamp_utc
            self.last_successful_rollup_utc = normalized
            self.last_successful_kpi_summary_utc = normalized
            self.last_rollup_error = None
        elif error:
            self.last_rollup_error = error

    def mark_cleanup(self, timestamp_utc: str, *, success: bool, error: str | None = None) -> None:
        if success:
            self.last_successful_cleanup_utc = normalize_storage_timestamp(timestamp_utc) or timestamp_utc
            self.last_cleanup_error = None
        elif error:
            self.last_cleanup_error = error

    def get_status(self) -> dict[str, Any]:
        return {
            "analytics_refresh_interval_seconds": self.settings.analytics_refresh_interval_seconds,
            "poll_interval_seconds": self.settings.poll_interval_seconds,
            "raw_write_interval_seconds": self.settings.raw_write_interval_seconds,
            "derived_write_interval_seconds": self.settings.derived_write_interval_seconds,
            "rollup_interval_seconds": self.settings.rollup_interval_seconds,
            "retention_days_raw": self.settings.retention_days_raw,
            "retention_days_rollup": self.settings.retention_days_rollup,
            "persistence_enabled": self.settings.persistence_enabled,
            "live_refresh_enabled": self.settings.live_refresh_enabled,
            "cleanup_enabled": self.settings.cleanup_enabled,
            "last_successful_poll_utc": self.last_successful_poll_utc,
            "last_successful_raw_write_utc": self.last_successful_raw_write_utc,
            "last_successful_derived_write_utc": self.last_successful_derived_write_utc,
            "last_successful_rollup_utc": self.last_successful_rollup_utc,
            "last_successful_kpi_summary_utc": self.last_successful_kpi_summary_utc,
            "last_successful_cleanup_utc": self.last_successful_cleanup_utc,
            "last_poll_error": self.last_poll_error,
            "last_raw_write_error": self.last_raw_write_error,
            "last_derived_write_error": self.last_derived_write_error,
            "last_rollup_error": self.last_rollup_error,
            "last_cleanup_error": self.last_cleanup_error,
            "last_plan_utc": self.last_plan_utc,
            "last_plan": dict(self.last_plan),
            "live_view_updating": self.settings.live_refresh_enabled,
            "persistence_status": "enabled" if self.settings.persistence_enabled else "disabled",
        }

    def _is_due(self, last_success_utc: str | None, interval_seconds: int, timestamp_utc: str) -> bool:
        if interval_seconds <= 0:
            return True
        if not last_success_utc:
            return True
        current = parse_utc_timestamp(timestamp_utc)
        previous = parse_utc_timestamp(last_success_utc)
        if current is None or previous is None:
            return True
        return (current - previous).total_seconds() >= interval_seconds


def load_runtime_timing_settings(config: dict[str, Any]) -> RuntimeTimingSettings:
    scheduling = config.get("scheduling", {}) or {}
    polling = config.get("polling", {}) or {}
    analytics = config.get("analytics", {}) or {}
    return RuntimeTimingSettings(
        analytics_refresh_interval_seconds=_to_positive_int(
            scheduling.get("analytics_refresh_interval_seconds"),
            analytics.get("chart_refresh_seconds", 30),
        ),
        poll_interval_seconds=_to_positive_int(
            scheduling.get("poll_interval_seconds"),
            polling.get("interval_seconds", 10),
        ),
        raw_write_interval_seconds=_to_positive_int(
            scheduling.get("raw_write_interval_seconds"),
            polling.get("interval_seconds", 10),
        ),
        derived_write_interval_seconds=_to_positive_int(
            scheduling.get("derived_write_interval_seconds"),
            polling.get("interval_seconds", 10),
        ),
        rollup_interval_seconds=_to_positive_int(
            scheduling.get("rollup_interval_seconds"),
            60,
        ),
        retention_days_raw=_to_positive_int(
            scheduling.get("retention_days_raw"),
            30,
        ),
        retention_days_rollup=_to_positive_int(
            scheduling.get("retention_days_rollup"),
            analytics.get("rollup_retention_days", 180),
        ),
        persistence_enabled=bool(scheduling.get("persistence_enabled", True)),
        live_refresh_enabled=bool(scheduling.get("live_refresh_enabled", True)),
        cleanup_enabled=bool(scheduling.get("cleanup_enabled", True)),
    )


def build_timing_baseline(store: Any) -> dict[str, Any]:
    analytics_status = store.get_analytics_status()
    return {
        "last_successful_poll_utc": store.get_latest_poll_timestamp(success_only=True),
        "last_successful_raw_write_utc": store.get_last_measurement_timestamp(),
        "last_successful_derived_write_utc": analytics_status.get("latest_semantic_metric_utc"),
        "last_successful_rollup_utc": analytics_status.get("latest_rollup_utc"),
        "last_successful_kpi_summary_utc": analytics_status.get("latest_kpi_summary_utc"),
        "last_successful_cleanup_utc": store.get_latest_cleanup_timestamp(success_only=True),
    }


def _to_positive_int(value: Any, fallback: int) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        return int(fallback)
    return parsed if parsed > 0 else int(fallback)
