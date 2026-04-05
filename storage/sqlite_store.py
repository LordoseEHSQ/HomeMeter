from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from collectors.base import CollectorResult, MeasurementRecord
from services.time_utils import normalize_storage_timestamp, parse_utc_timestamp


@dataclass(slots=True)
class SemanticMetricRecord:
    timestamp_utc: str
    device_name: str
    metric_name: str
    metric_value: float
    unit: str | None
    classification: str
    formula_version: str | None
    source_metric_names: list[str]
    source_coverage: float
    confidence_state: str
    details: dict[str, Any]


@dataclass(slots=True)
class MinuteRollupRecord:
    bucket_utc: str
    device_name: str
    metric_name: str
    unit: str | None
    classification: str
    min_value: float
    max_value: float
    avg_value: float
    last_value: float
    sample_count: int
    source_coverage: float
    confidence_state: str
    updated_at_utc: str


@dataclass(slots=True)
class KPIRecord:
    window_key: str
    window_start_utc: str
    window_end_utc: str
    metric_name: str
    metric_value: float
    unit: str
    classification: str
    formula_version: str | None
    source_coverage: float
    confidence_state: str
    updated_at_utc: str
    details: dict[str, Any]


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def initialize(self) -> None:
        with self._managed_connection() as connection:
            cursor = connection.cursor()
            cursor.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS measurements (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_utc TEXT NOT NULL,
                    device_name TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    unit TEXT,
                    raw_payload TEXT
                );
                CREATE TABLE IF NOT EXISTS poll_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_utc TEXT NOT NULL,
                    device_name TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    http_status INTEGER,
                    error_message TEXT,
                    status TEXT NOT NULL,
                    raw_payload TEXT,
                    details_json TEXT
                );
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_utc TEXT NOT NULL,
                    device_name TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    rule_name TEXT NOT NULL,
                    message TEXT NOT NULL,
                    context_json TEXT
                );
                CREATE TABLE IF NOT EXISTS semantic_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp_utc TEXT NOT NULL,
                    device_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    unit TEXT,
                    classification TEXT NOT NULL,
                    formula_version TEXT,
                    source_metric_names_json TEXT,
                    source_coverage REAL,
                    confidence_state TEXT,
                    details_json TEXT
                );
                CREATE TABLE IF NOT EXISTS minute_rollups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bucket_utc TEXT NOT NULL,
                    device_name TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    unit TEXT,
                    classification TEXT NOT NULL,
                    min_value REAL NOT NULL,
                    max_value REAL NOT NULL,
                    avg_value REAL NOT NULL,
                    last_value REAL NOT NULL,
                    sample_count INTEGER NOT NULL,
                    source_coverage REAL,
                    confidence_state TEXT,
                    updated_at_utc TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS kpi_summaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    window_key TEXT NOT NULL,
                    window_start_utc TEXT NOT NULL,
                    window_end_utc TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    unit TEXT,
                    classification TEXT NOT NULL,
                    formula_version TEXT,
                    source_coverage REAL,
                    confidence_state TEXT,
                    updated_at_utc TEXT NOT NULL,
                    details_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_measurements_device_time
                ON measurements (device_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_measurements_metric_time
                ON measurements (metric_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_poll_events_device_time
                ON poll_events (device_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_alerts_device_time
                ON alerts (device_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_semantic_metrics_metric_time
                ON semantic_metrics (metric_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_semantic_metrics_device_time
                ON semantic_metrics (device_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_minute_rollups_metric_bucket
                ON minute_rollups (metric_name, bucket_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_minute_rollups_device_bucket
                ON minute_rollups (device_name, bucket_utc DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_kpi_summaries_window_metric
                ON kpi_summaries (window_key, metric_name);
                """
            )
            self._ensure_column(connection, "poll_events", "details_json", "TEXT")
            connection.commit()

    def save_collector_result(self, result: CollectorResult) -> None:
        with self._lock, self._managed_connection() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO poll_events (
                    timestamp_utc, device_name, success, duration_ms, http_status,
                    error_message, status, raw_payload, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalize_storage_timestamp(result.timestamp_utc),
                    result.device_name,
                    int(result.success),
                    result.duration_ms,
                    result.http_status,
                    result.error_message,
                    result.status.value,
                    result.raw_payload,
                    json.dumps(result.details or {}),
                ),
            )
            for measurement in result.measurements:
                self._insert_measurement(
                    cursor,
                    normalize_storage_timestamp(result.timestamp_utc) or result.timestamp_utc,
                    result.device_name,
                    measurement,
                )
            connection.commit()

    def save_alert(
        self,
        timestamp_utc: str,
        device_name: str,
        severity: str,
        rule_name: str,
        message: str,
        context: dict[str, Any],
    ) -> None:
        with self._lock, self._managed_connection() as connection:
            connection.execute(
                """
                INSERT INTO alerts (
                    timestamp_utc, device_name, severity, rule_name, message, context_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    normalize_storage_timestamp(timestamp_utc) or timestamp_utc,
                    device_name,
                    severity,
                    rule_name,
                    message,
                    json.dumps(context),
                ),
            )
            connection.commit()

    def save_semantic_metrics(self, records: list[SemanticMetricRecord]) -> None:
        if not records:
            return
        with self._lock, self._managed_connection() as connection:
            cursor = connection.cursor()
            cursor.executemany(
                """
                INSERT INTO semantic_metrics (
                    timestamp_utc, device_name, metric_name, metric_value, unit,
                    classification, formula_version, source_metric_names_json,
                    source_coverage, confidence_state, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        normalize_storage_timestamp(record.timestamp_utc) or record.timestamp_utc,
                        record.device_name,
                        record.metric_name,
                        record.metric_value,
                        record.unit,
                        record.classification,
                        record.formula_version,
                        json.dumps(record.source_metric_names),
                        record.source_coverage,
                        record.confidence_state,
                        json.dumps(record.details or {}),
                    )
                    for record in records
                ],
            )
            connection.commit()

    def refresh_minute_rollups(self, bucket_utc: str, metric_names: list[str], device_name: str = "system") -> list[MinuteRollupRecord]:
        if not metric_names:
            return []
        bucket_start = normalize_storage_timestamp(bucket_utc) or bucket_utc
        parsed = parse_utc_timestamp(bucket_start) or datetime.now(timezone.utc)
        bucket_end = normalize_storage_timestamp(parsed + timedelta(minutes=1)) or bucket_start
        placeholders = ",".join("?" for _ in metric_names)
        with self._lock, self._managed_connection() as connection:
            connection.execute(
                f"DELETE FROM minute_rollups WHERE device_name = ? AND bucket_utc = ? AND metric_name IN ({placeholders})",
                [device_name, bucket_start, *metric_names],
            )
            rows = connection.execute(
                f"""
                SELECT metric_name,
                       MIN(metric_value) AS min_value,
                       MAX(metric_value) AS max_value,
                       AVG(metric_value) AS avg_value,
                       COUNT(*) AS sample_count,
                       MAX(timestamp_utc) AS last_ts,
                       AVG(COALESCE(source_coverage, 0)) AS avg_coverage,
                       unit
                FROM semantic_metrics
                WHERE device_name = ? AND timestamp_utc >= ? AND timestamp_utc < ? AND metric_name IN ({placeholders})
                GROUP BY metric_name, unit
                """,
                [device_name, bucket_start, bucket_end, *metric_names],
            ).fetchall()
            created: list[MinuteRollupRecord] = []
            for row in rows:
                classification_row = connection.execute(
                    """
                    SELECT classification, confidence_state, metric_value
                    FROM semantic_metrics
                    WHERE device_name = ? AND metric_name = ? AND timestamp_utc >= ? AND timestamp_utc < ?
                    ORDER BY timestamp_utc DESC
                    """,
                    (device_name, row["metric_name"], bucket_start, bucket_end),
                ).fetchall()
                if not classification_row:
                    continue
                last_value = float(classification_row[0]["metric_value"])
                classifications = [item["classification"] for item in classification_row]
                confidences = [item["confidence_state"] for item in classification_row if item["confidence_state"]]
                rollup = MinuteRollupRecord(
                    bucket_utc=bucket_start,
                    device_name=device_name,
                    metric_name=row["metric_name"],
                    unit=row["unit"],
                    classification=self._worst_classification(classifications),
                    min_value=float(row["min_value"]),
                    max_value=float(row["max_value"]),
                    avg_value=float(row["avg_value"]),
                    last_value=last_value,
                    sample_count=int(row["sample_count"]),
                    source_coverage=float(row["avg_coverage"] or 0.0),
                    confidence_state=self._worst_confidence(confidences),
                    updated_at_utc=normalize_storage_timestamp(datetime.now(timezone.utc)) or bucket_start,
                )
                connection.execute(
                    """
                    INSERT INTO minute_rollups (
                        bucket_utc, device_name, metric_name, unit, classification,
                        min_value, max_value, avg_value, last_value, sample_count,
                        source_coverage, confidence_state, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rollup.bucket_utc,
                        rollup.device_name,
                        rollup.metric_name,
                        rollup.unit,
                        rollup.classification,
                        rollup.min_value,
                        rollup.max_value,
                        rollup.avg_value,
                        rollup.last_value,
                        rollup.sample_count,
                        rollup.source_coverage,
                        rollup.confidence_state,
                        rollup.updated_at_utc,
                    ),
                )
                created.append(rollup)
            connection.commit()
        return created

    def upsert_kpi_summaries(self, records: list[KPIRecord]) -> None:
        if not records:
            return
        with self._lock, self._managed_connection() as connection:
            connection.executemany(
                """
                INSERT INTO kpi_summaries (
                    window_key, window_start_utc, window_end_utc, metric_name,
                    metric_value, unit, classification, formula_version,
                    source_coverage, confidence_state, updated_at_utc, details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(window_key, metric_name) DO UPDATE SET
                    window_start_utc = excluded.window_start_utc,
                    window_end_utc = excluded.window_end_utc,
                    metric_value = excluded.metric_value,
                    unit = excluded.unit,
                    classification = excluded.classification,
                    formula_version = excluded.formula_version,
                    source_coverage = excluded.source_coverage,
                    confidence_state = excluded.confidence_state,
                    updated_at_utc = excluded.updated_at_utc,
                    details_json = excluded.details_json
                """,
                [
                    (
                        record.window_key,
                        record.window_start_utc,
                        record.window_end_utc,
                        record.metric_name,
                        record.metric_value,
                        record.unit,
                        record.classification,
                        record.formula_version,
                        record.source_coverage,
                        record.confidence_state,
                        record.updated_at_utc,
                        json.dumps(record.details or {}),
                    )
                    for record in records
                ],
            )
            connection.commit()

    def get_dashboard_data(self, device_names: list[str], measurement_limit: int = 10) -> dict[str, Any]:
        with self._managed_connection() as connection:
            devices = []
            for device_name in device_names:
                devices.append(
                    {
                        "name": device_name,
                        "last_poll": self._get_last_poll(connection, device_name),
                        "last_successful_poll": self._get_last_successful_poll(connection, device_name),
                        "latest_measurements": self._get_latest_measurements(connection, device_name, measurement_limit),
                        "recent_errors": self._get_recent_errors(connection, device_name, limit=3),
                    }
                )
            return {"devices": devices, "recent_alerts": self.get_recent_alerts(limit=20)}

    def get_device_detail(self, device_name: str, history_limit: int = 50) -> dict[str, Any]:
        with self._managed_connection() as connection:
            return {
                "name": device_name,
                "last_poll": self._get_last_poll(connection, device_name),
                "last_successful_poll": self._get_last_successful_poll(connection, device_name),
                "poll_history": self._get_poll_history(connection, device_name, history_limit),
                "measurements": self._get_measurement_history(connection, device_name, history_limit),
                "raw_payload": self._get_latest_raw_payload(connection, device_name),
                "recent_errors": self._get_recent_errors(connection, device_name, limit=10),
            }

    def get_recent_alerts(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._managed_connection() as connection:
            rows = connection.execute(
                """
                SELECT id, timestamp_utc, device_name, severity, rule_name, message, context_json
                FROM alerts ORDER BY timestamp_utc DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_recent_measurements(
        self, metric_name: str, device_name: str | None = None, limit: int = 10
    ) -> list[dict[str, Any]]:
        query = """
            SELECT timestamp_utc, device_name, source_type, metric_name, metric_value, unit, raw_payload
            FROM measurements WHERE metric_name = ?
        """
        params: list[Any] = [metric_name]
        if device_name:
            query += " AND device_name = ?"
            params.append(device_name)
        query += " ORDER BY timestamp_utc DESC LIMIT ?"
        params.append(limit)
        with self._managed_connection() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_latest_measurement_value(self, metric_name: str, device_name: str | None = None) -> float | None:
        rows = self.get_recent_measurements(metric_name=metric_name, device_name=device_name, limit=1)
        return rows[0]["metric_value"] if rows else None

    def get_latest_measurement_row(self, metric_name: str, device_name: str | None = None) -> dict[str, Any] | None:
        rows = self.get_recent_measurements(metric_name=metric_name, device_name=device_name, limit=1)
        return rows[0] if rows else None

    def get_recent_poll_events(self, device_name: str, limit: int = 5) -> list[dict[str, Any]]:
        with self._managed_connection() as connection:
            rows = connection.execute(
                """
                SELECT timestamp_utc, device_name, success, duration_ms, http_status, error_message, status, details_json
                FROM poll_events WHERE device_name = ? ORDER BY timestamp_utc DESC LIMIT ?
                """,
                (device_name, limit),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_latest_raw_payload(self, device_name: str) -> dict[str, Any] | None:
        with self._managed_connection() as connection:
            return self._get_latest_raw_payload(connection, device_name)

    def get_recent_success_poll(self, device_name: str) -> dict[str, Any] | None:
        with self._managed_connection() as connection:
            row = connection.execute(
                """
                SELECT timestamp_utc, device_name, success, duration_ms, http_status, error_message, status, details_json
                FROM poll_events
                WHERE device_name = ? AND success = 1
                ORDER BY timestamp_utc DESC
                LIMIT 1
                """,
                (device_name,),
            ).fetchone()
        return self._row_to_dict(row) if row else None

    def count_recent_failures(self, device_name: str, limit: int = 5) -> int:
        events = self.get_recent_poll_events(device_name, limit=limit)
        return sum(1 for event in events if not event["success"])

    def get_metric_count(self, device_name: str) -> int:
        with self._managed_connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS row_count FROM measurements WHERE device_name = ?",
                (device_name,),
            ).fetchone()
        return int(row["row_count"]) if row else 0

    def get_last_measurement_timestamp(self, device_name: str) -> str | None:
        with self._managed_connection() as connection:
            row = connection.execute(
                """
                SELECT MAX(timestamp_utc) AS latest_measurement_ts
                FROM measurements
                WHERE device_name = ?
                """,
                (device_name,),
            ).fetchone()
        return row["latest_measurement_ts"] if row and row["latest_measurement_ts"] else None

    def has_raw_payload(self, device_name: str) -> bool:
        with self._managed_connection() as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM poll_events
                WHERE device_name = ? AND raw_payload IS NOT NULL AND raw_payload != ''
                LIMIT 1
                """,
                (device_name,),
            ).fetchone()
        return row is not None

    def count_raw_payload_rows(self, device_name: str | None = None) -> int:
        query = """
            SELECT COUNT(*) AS row_count
            FROM poll_events
            WHERE raw_payload IS NOT NULL AND raw_payload != ''
        """
        params: list[Any] = []
        if device_name:
            query += " AND device_name = ?"
            params.append(device_name)
        with self._managed_connection() as connection:
            row = connection.execute(query, params).fetchone()
        return int(row["row_count"]) if row else 0

    def device_has_measurements(self, device_name: str) -> bool:
        return self.get_metric_count(device_name) > 0

    def count_poll_events(self, device_name: str) -> int:
        with self._managed_connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS row_count FROM poll_events WHERE device_name = ?",
                (device_name,),
            ).fetchone()
        return int(row["row_count"]) if row else 0

    def count_measurements(self, device_name: str, source_type: str | None = None) -> int:
        query = "SELECT COUNT(*) AS row_count FROM measurements WHERE device_name = ?"
        params: list[Any] = [device_name]
        if source_type:
            query += " AND source_type = ?"
            params.append(source_type)
        with self._managed_connection() as connection:
            row = connection.execute(query, params).fetchone()
        return int(row["row_count"]) if row else 0

    def get_recording_summary(self, device_name: str) -> dict[str, Any]:
        poll_count = self.count_poll_events(device_name)
        raw_payload_count = self.count_raw_payload_rows(device_name)
        measurement_count = self.count_measurements(device_name)
        raw_numeric_count = self.count_measurements(device_name, source_type="raw_numeric")
        unmapped_numeric_count = self.count_measurements(device_name, source_type="unmapped_numeric")
        likely_candidate_count = self.count_measurements(device_name, source_type="likely_useful_candidate")
        confirmed_useful_count = self.count_measurements(device_name, source_type="confirmed_useful")
        verified_count = self.count_measurements(device_name, source_type="verified")
        tentative_count = self.count_measurements(device_name, source_type="tentative")
        normalized_count = confirmed_useful_count + verified_count + tentative_count
        last_poll = self.get_recent_poll_events(device_name, limit=1)
        last_measurement_ts = self.get_last_measurement_timestamp(device_name)
        if poll_count == 0:
            status = "idle"
            message = "No poll events stored yet."
        elif raw_payload_count == 0:
            status = "warning"
            message = "Poll events exist but no raw payload is stored yet."
        elif normalized_count == 0 and (raw_numeric_count > 0 or unmapped_numeric_count > 0 or likely_candidate_count > 0):
            status = "partial"
            message = "Payload data is being stored, but trusted normalized metrics are still missing."
        elif normalized_count == 0:
            status = "warning"
            message = "Raw payload exists but no measurements are stored yet."
        else:
            status = "healthy"
            message = "Poll events, raw payloads, and normalized measurements are all increasing."
        return {
            "status": status,
            "message": message,
            "poll_event_count": poll_count,
            "raw_payload_count": raw_payload_count,
            "measurement_count": measurement_count,
            "normalized_measurement_count": normalized_count,
            "raw_numeric_measurement_count": raw_numeric_count,
            "unmapped_numeric_measurement_count": unmapped_numeric_count,
            "likely_candidate_measurement_count": likely_candidate_count,
            "confirmed_useful_measurement_count": confirmed_useful_count,
            "verified_measurement_count": verified_count,
            "tentative_measurement_count": tentative_count,
            "last_poll_timestamp_utc": last_poll[0]["timestamp_utc"] if last_poll else None,
            "last_measurement_timestamp_utc": last_measurement_ts,
        }

    def get_kpi_summaries(self, window_key: str) -> list[dict[str, Any]]:
        with self._managed_connection() as connection:
            rows = connection.execute(
                """
                SELECT window_key, window_start_utc, window_end_utc, metric_name, metric_value,
                       unit, classification, formula_version, source_coverage,
                       confidence_state, updated_at_utc, details_json
                FROM kpi_summaries
                WHERE window_key = ?
                ORDER BY metric_name ASC
                """,
                (window_key,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_rollup_series(
        self,
        metric_name: str,
        start_utc: datetime,
        end_utc: datetime,
        *,
        device_name: str = "system",
        limit: int = 240,
    ) -> list[dict[str, Any]]:
        with self._managed_connection() as connection:
            rows = connection.execute(
                """
                SELECT bucket_utc, metric_name, unit, classification, min_value, max_value,
                       avg_value, last_value, sample_count, source_coverage, confidence_state, updated_at_utc
                FROM minute_rollups
                WHERE device_name = ? AND metric_name = ? AND bucket_utc >= ? AND bucket_utc <= ?
                ORDER BY bucket_utc ASC
                LIMIT ?
                """,
                (
                    device_name,
                    metric_name,
                    normalize_storage_timestamp(start_utc) or str(start_utc),
                    normalize_storage_timestamp(end_utc) or str(end_utc),
                    limit,
                ),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def aggregate_rollup_window(self, metric_name: str, start_utc: datetime, end_utc: datetime, device_name: str = "system") -> dict[str, Any]:
        start_ts = normalize_storage_timestamp(start_utc) or str(start_utc)
        end_ts = normalize_storage_timestamp(end_utc) or str(end_utc)
        expected_count = max(int((end_utc - start_utc).total_seconds() // 60), 1)
        with self._managed_connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS sample_count,
                       AVG(avg_value) AS mean_avg,
                       MAX(max_value) AS peak_max,
                       SUM(avg_value) AS sum_avg,
                       AVG(COALESCE(source_coverage, 0)) AS avg_coverage
                FROM minute_rollups
                WHERE device_name = ? AND metric_name = ? AND bucket_utc >= ? AND bucket_utc <= ?
                """,
                (device_name, metric_name, start_ts, end_ts),
            ).fetchone()
            class_rows = connection.execute(
                """
                SELECT classification, confidence_state
                FROM minute_rollups
                WHERE device_name = ? AND metric_name = ? AND bucket_utc >= ? AND bucket_utc <= ?
                ORDER BY bucket_utc DESC
                """,
                (device_name, metric_name, start_ts, end_ts),
            ).fetchall()
        sample_count = int(row["sample_count"] or 0) if row else 0
        coverage = min(sample_count / expected_count, 1.0) if expected_count else 0.0
        classifications = [item["classification"] for item in class_rows]
        confidences = [item["confidence_state"] for item in class_rows if item["confidence_state"]]
        return {
            "sample_count": sample_count,
            "expected_count": expected_count,
            "coverage": round(coverage, 3),
            "average_power_w": float(row["mean_avg"]) if row and row["mean_avg"] is not None else None,
            "peak_power_w": float(row["peak_max"]) if row and row["peak_max"] is not None else None,
            "energy_kwh": (float(row["sum_avg"]) / 1000.0 / 60.0) if row and row["sum_avg"] is not None else None,
            "classification": self._worst_classification(classifications),
            "confidence_state": self._worst_confidence(confidences),
            "average_source_coverage": float(row["avg_coverage"] or 0.0) if row else 0.0,
        }

    def get_analytics_status(self) -> dict[str, Any]:
        with self._managed_connection() as connection:
            latest_measurement = connection.execute("SELECT MAX(timestamp_utc) AS ts FROM measurements").fetchone()
            latest_semantic = connection.execute("SELECT MAX(timestamp_utc) AS ts FROM semantic_metrics").fetchone()
            latest_rollup = connection.execute("SELECT MAX(updated_at_utc) AS ts FROM minute_rollups").fetchone()
            latest_kpi = connection.execute("SELECT MAX(updated_at_utc) AS ts FROM kpi_summaries").fetchone()
            semantic_count = connection.execute("SELECT COUNT(*) AS row_count FROM semantic_metrics").fetchone()
            rollup_count = connection.execute("SELECT COUNT(*) AS row_count FROM minute_rollups").fetchone()
            kpi_count = connection.execute("SELECT COUNT(*) AS row_count FROM kpi_summaries").fetchone()
        return {
            "latest_raw_measurement_utc": latest_measurement["ts"] if latest_measurement else None,
            "latest_semantic_metric_utc": latest_semantic["ts"] if latest_semantic else None,
            "latest_rollup_utc": latest_rollup["ts"] if latest_rollup else None,
            "latest_kpi_summary_utc": latest_kpi["ts"] if latest_kpi else None,
            "semantic_metric_count": int(semantic_count["row_count"]) if semantic_count else 0,
            "rollup_count": int(rollup_count["row_count"]) if rollup_count else 0,
            "kpi_summary_count": int(kpi_count["row_count"]) if kpi_count else 0,
        }

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        if table_name not in {"measurements", "poll_events", "alerts", "semantic_metrics", "minute_rollups", "kpi_summaries"}:
            raise ValueError(f"Unsupported table name: {table_name}")
        timestamp_column = {
            "minute_rollups": "bucket_utc",
            "kpi_summaries": "updated_at_utc",
        }.get(table_name, "timestamp_utc")
        with self._managed_connection() as connection:
            count_row = connection.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}").fetchone()
            time_row = connection.execute(
                f"SELECT MIN({timestamp_column}) AS oldest_ts, MAX({timestamp_column}) AS newest_ts FROM {table_name}"
            ).fetchone()
        return {
            "row_count": int(count_row["row_count"]) if count_row else 0,
            "oldest_ts": time_row["oldest_ts"] if time_row else None,
            "newest_ts": time_row["newest_ts"] if time_row else None,
        }

    def get_latest_rows(self, table_name: str, limit: int = 10) -> list[dict[str, Any]]:
        if table_name not in {"measurements", "poll_events", "alerts", "semantic_metrics", "minute_rollups", "kpi_summaries"}:
            raise ValueError(f"Unsupported table name: {table_name}")
        order_column = {
            "minute_rollups": "bucket_utc",
            "kpi_summaries": "updated_at_utc",
        }.get(table_name, "timestamp_utc")
        with self._managed_connection() as connection:
            rows = connection.execute(
                f"SELECT * FROM {table_name} ORDER BY {order_column} DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _insert_measurement(
        self,
        cursor: sqlite3.Cursor,
        timestamp_utc: str,
        device_name: str,
        measurement: MeasurementRecord,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO measurements (
                timestamp_utc, device_name, source_type, metric_name, metric_value, unit, raw_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp_utc,
                device_name,
                measurement.source_type,
                measurement.metric_name,
                measurement.metric_value,
                measurement.unit,
                measurement.raw_payload,
            ),
        )

    def _get_last_poll(self, connection: sqlite3.Connection, device_name: str) -> dict[str, Any] | None:
        row = connection.execute(
            """
            SELECT timestamp_utc, success, duration_ms, http_status, error_message, status
            FROM poll_events WHERE device_name = ? ORDER BY timestamp_utc DESC LIMIT 1
            """,
            (device_name,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def _get_last_successful_poll(
        self, connection: sqlite3.Connection, device_name: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            """
            SELECT timestamp_utc, success, duration_ms, http_status, error_message, status
            FROM poll_events WHERE device_name = ? AND success = 1
            ORDER BY timestamp_utc DESC LIMIT 1
            """,
            (device_name,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def _get_latest_measurements(
        self, connection: sqlite3.Connection, device_name: str, limit: int
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT m.timestamp_utc, m.metric_name, m.metric_value, m.unit, m.source_type
            FROM measurements m
            INNER JOIN (
                SELECT metric_name, MAX(timestamp_utc) AS max_ts
                FROM measurements WHERE device_name = ? GROUP BY metric_name
            ) latest
            ON latest.metric_name = m.metric_name AND latest.max_ts = m.timestamp_utc
            WHERE m.device_name = ?
            ORDER BY m.metric_name ASC LIMIT ?
            """,
            (device_name, device_name, limit),
        ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _get_recent_errors(
        self, connection: sqlite3.Connection, device_name: str, limit: int
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT timestamp_utc, http_status, error_message, status
            FROM poll_events
            WHERE device_name = ? AND (success = 0 OR error_message IS NOT NULL)
            ORDER BY timestamp_utc DESC LIMIT ?
            """,
            (device_name, limit),
        ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _get_poll_history(
        self, connection: sqlite3.Connection, device_name: str, limit: int
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT timestamp_utc, success, duration_ms, http_status, error_message, status
            FROM poll_events WHERE device_name = ? ORDER BY timestamp_utc DESC LIMIT ?
            """,
            (device_name, limit),
        ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def _get_measurement_history(
        self, connection: sqlite3.Connection, device_name: str, limit: int
    ) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT timestamp_utc, metric_name, metric_value, unit, source_type
            FROM measurements WHERE device_name = ?
            ORDER BY timestamp_utc DESC, metric_name ASC LIMIT ?
            """,
            (device_name, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def _get_latest_raw_payload(
        self, connection: sqlite3.Connection, device_name: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            """
            SELECT timestamp_utc, raw_payload, status, error_message, details_json
            FROM poll_events
            WHERE device_name = ? AND raw_payload IS NOT NULL AND raw_payload != ''
            ORDER BY timestamp_utc DESC LIMIT 1
            """,
            (device_name,),
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _managed_connection(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def _ensure_column(self, connection: sqlite3.Connection, table_name: str, column_name: str, column_type: str) -> None:
        columns = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing_names = {row["name"] for row in columns}
        if column_name not in existing_names:
            connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def _row_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            return {}
        data = dict(row)
        for json_field, parsed_name in {
            "details_json": "details",
            "source_metric_names_json": "source_metric_names",
            "context_json": "context",
        }.items():
            json_value = data.pop(json_field, None)
            if json_value:
                try:
                    data[parsed_name] = json.loads(json_value)
                except json.JSONDecodeError:
                    data[parsed_name] = {"parse_error": f"Could not decode stored {json_field}."}
        return data

    def _worst_classification(self, classifications: list[str]) -> str:
        if not classifications:
            return "estimated"
        if "estimated" in classifications:
            return "estimated"
        if "derived" in classifications:
            return "derived"
        return "measured"

    def _worst_confidence(self, confidences: list[str]) -> str:
        if not confidences:
            return "unknown"
        if "incomplete" in confidences:
            return "incomplete"
        if "tentative" in confidences:
            return "tentative"
        if "medium" in confidences:
            return "medium"
        if "high" in confidences:
            return "high"
        return confidences[0]
