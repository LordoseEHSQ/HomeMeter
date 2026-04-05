from __future__ import annotations

from typing import Any

from services.config_validation import ConfigValidationResult
from storage.sqlite_store import SQLiteStore


class SystemHealthService:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def build_summary(
        self,
        device_operations: list[dict[str, Any]],
        config_validation: ConfigValidationResult,
        database_report: dict[str, Any],
    ) -> dict[str, Any]:
        latest_alerts = self.store.get_recent_alerts(limit=20)
        high_alerts = sum(1 for alert in latest_alerts if alert["severity"] == "high")
        unhealthy_devices = [
            device for device in device_operations if device["collector_status"] not in {"healthy", "reachable"}
        ]
        stale_devices = [
            device["name"]
            for device in device_operations
            if device["last_successful_poll"] is None and device["enabled"]
        ]

        if config_validation.error_count or database_report["storage_activity"]["status"] == "error":
            status = "error"
        elif high_alerts or any(device["collector_status"] in {"unreachable", "timeout", "auth_failed"} for device in device_operations):
            status = "degraded"
        elif config_validation.warning_count or unhealthy_devices:
            status = "warning"
        else:
            status = "healthy"

        return {
            "status": status,
            "config_status": config_validation.status,
            "alert_count": len(latest_alerts),
            "high_alert_count": high_alerts,
            "unhealthy_device_count": len(unhealthy_devices),
            "stale_devices": stale_devices,
            "storage_status": database_report["storage_activity"]["status"],
            "storage_message": database_report["storage_activity"]["message"],
            "recent_failures": sum(device["recent_failure_count"] for device in device_operations),
        }
