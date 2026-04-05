from __future__ import annotations

from pathlib import Path
from typing import Any

from storage.sqlite_store import SQLiteStore


class DatabaseInspector:
    def __init__(self, store: SQLiteStore) -> None:
        self.store = store

    def build_report(self) -> dict[str, Any]:
        db_path = Path(self.store.db_path)
        table_names = ("measurements", "poll_events", "alerts", "semantic_metrics", "minute_rollups", "kpi_summaries")
        table_stats = {name: self.store.get_table_stats(name) for name in table_names}
        return {
            "db_path": str(db_path.resolve()),
            "file_exists": db_path.exists(),
            "file_size_bytes": db_path.stat().st_size if db_path.exists() else 0,
            "tables": table_stats,
            "latest_rows": {
                "measurements": self.store.get_latest_rows("measurements", limit=10),
                "poll_events": self.store.get_latest_rows("poll_events", limit=10),
                "alerts": self.store.get_latest_rows("alerts", limit=10),
                "semantic_metrics": self.store.get_latest_rows("semantic_metrics", limit=10),
                "minute_rollups": self.store.get_latest_rows("minute_rollups", limit=10),
                "kpi_summaries": self.store.get_latest_rows("kpi_summaries", limit=10),
            },
            "storage_activity": self._build_activity_summary(table_stats),
            "device_measurement_presence": {
                "cfos": self.store.device_has_measurements("cfos"),
                "easee": self.store.device_has_measurements("easee"),
                "kostal": self.store.device_has_measurements("kostal"),
            },
            "raw_payload_rows": self.store.count_raw_payload_rows(),
            "per_device_recording": {
                "cfos": self.store.get_recording_summary("cfos"),
                "easee": self.store.get_recording_summary("easee"),
                "kostal": self.store.get_recording_summary("kostal"),
            },
            "analytics_status": self.store.get_analytics_status(),
        }

    def _build_activity_summary(self, table_stats: dict[str, dict[str, Any]]) -> dict[str, Any]:
        measurement_count = table_stats["measurements"]["row_count"]
        poll_count = table_stats["poll_events"]["row_count"]
        alert_count = table_stats["alerts"]["row_count"]
        semantic_count = table_stats["semantic_metrics"]["row_count"]
        rollup_count = table_stats["minute_rollups"]["row_count"]
        kpi_count = table_stats["kpi_summaries"]["row_count"]
        if poll_count == 0:
            status = "error"
            message = "No poll events stored yet."
        elif measurement_count == 0:
            status = "warning"
            message = "Poll events exist but no measurements are stored yet."
        elif semantic_count == 0 or rollup_count == 0:
            status = "warning"
            message = "Raw measurements exist, but analytics persistence has not fully started yet."
        else:
            status = "healthy"
            message = "Database is receiving poll events, measurements, derived metrics, rollups, and KPI summaries."
        return {
            "status": status,
            "message": message,
            "measurement_count": measurement_count,
            "poll_event_count": poll_count,
            "alert_count": alert_count,
            "semantic_metric_count": semantic_count,
            "rollup_count": rollup_count,
            "kpi_summary_count": kpi_count,
        }
