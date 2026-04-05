from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from collectors.base import CollectorResult, MeasurementRecord
from services.time_utils import normalize_storage_timestamp


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
                    raw_payload TEXT
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
                CREATE INDEX IF NOT EXISTS idx_measurements_device_time
                ON measurements (device_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_measurements_metric_time
                ON measurements (metric_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_poll_events_device_time
                ON poll_events (device_name, timestamp_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_alerts_device_time
                ON alerts (device_name, timestamp_utc DESC);
                """
            )
            connection.commit()

    def save_collector_result(self, result: CollectorResult) -> None:
        with self._lock, self._managed_connection() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                INSERT INTO poll_events (
                    timestamp_utc, device_name, success, duration_ms, http_status,
                    error_message, status, raw_payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
        return [dict(row) for row in rows]

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
        return [dict(row) for row in rows]

    def get_latest_measurement_value(self, metric_name: str, device_name: str | None = None) -> float | None:
        rows = self.get_recent_measurements(metric_name=metric_name, device_name=device_name, limit=1)
        return rows[0]["metric_value"] if rows else None

    def get_recent_poll_events(self, device_name: str, limit: int = 5) -> list[dict[str, Any]]:
        with self._managed_connection() as connection:
            rows = connection.execute(
                """
                SELECT timestamp_utc, device_name, success, duration_ms, http_status, error_message, status
                FROM poll_events WHERE device_name = ? ORDER BY timestamp_utc DESC LIMIT ?
                """,
                (device_name, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_recent_success_poll(self, device_name: str) -> dict[str, Any] | None:
        with self._managed_connection() as connection:
            row = connection.execute(
                """
                SELECT timestamp_utc, device_name, success, duration_ms, http_status, error_message, status
                FROM poll_events
                WHERE device_name = ? AND success = 1
                ORDER BY timestamp_utc DESC
                LIMIT 1
                """,
                (device_name,),
            ).fetchone()
        return dict(row) if row else None

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

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        if table_name not in {"measurements", "poll_events", "alerts"}:
            raise ValueError(f"Unsupported table name: {table_name}")
        timestamp_column = "timestamp_utc"
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
        if table_name not in {"measurements", "poll_events", "alerts"}:
            raise ValueError(f"Unsupported table name: {table_name}")
        with self._managed_connection() as connection:
            rows = connection.execute(
                f"SELECT * FROM {table_name} ORDER BY timestamp_utc DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

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
        return dict(row) if row else None

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
        return dict(row) if row else None

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
        return [dict(row) for row in rows]

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
        return [dict(row) for row in rows]

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
        return [dict(row) for row in rows]

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
            SELECT timestamp_utc, raw_payload, status, error_message
            FROM poll_events
            WHERE device_name = ? AND raw_payload IS NOT NULL AND raw_payload != ''
            ORDER BY timestamp_utc DESC LIMIT 1
            """,
            (device_name,),
        ).fetchone()
        return dict(row) if row else None

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
