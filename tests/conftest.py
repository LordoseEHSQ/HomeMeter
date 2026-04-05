from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from app import create_app
from collectors.base import CollectorResult, CollectorStatus, MeasurementRecord
from storage.sqlite_store import SQLiteStore


@pytest.fixture
def sample_config_dict(tmp_path: Path) -> dict:
    return {
        "app": {"host": "127.0.0.1", "port": 5001, "debug": False},
        "polling": {"interval_seconds": 10, "connect_timeout_seconds": 3, "read_timeout_seconds": 6},
        "storage": {"sqlite_path": str(tmp_path / "test.db")},
        "time": {
            "display_timezone": "Europe/Berlin",
            "display_format": "%d.%m.%Y %H:%M:%S",
            "ntp": {
                "enabled": False,
                "servers": ["0.de.pool.ntp.org", "1.de.pool.ntp.org"],
                "timeout_seconds": 2,
                "drift_warning_seconds": 2,
            },
        },
        "analytics": {
            "default_window": "24h",
            "chart_refresh_seconds": 30,
            "rollup_retention_days": 180,
        },
        "devices": {
            "cfos": {
                "enabled": True,
                "name": "cfos",
                "base_url": "http://127.0.0.1",
                "status_path": "/",
                "candidate_status_paths": ["/", "/status", "/api/status"],
                "preferred_protocols": ["http", "mqtt"],
                "auth": {
                    "enabled": True,
                    "type": "basic",
                    "credential_source": "custom",
                    "default_username": "admin",
                    "default_password_variants": ["", "1234abcd"],
                    "username": "admin",
                    "password": "secret",
                },
                "protocols": {
                    "http": {"enabled": True},
                    "mqtt": {"enabled": False, "host": "127.0.0.1", "port": 1883},
                    "modbus": {"enabled": False, "host": "127.0.0.1", "port": 502},
                    "sunspec": {"enabled": False, "host": "127.0.0.1", "port": 1502},
                },
                "timeout_seconds": 5,
            },
            "easee": {
                "enabled": True,
                "name": "easee",
                "base_url": "http://127.0.0.2",
                "status_path": "/",
                "auth": {"type": "none"},
                "timeout_seconds": 5,
            },
            "kostal": {
                "enabled": True,
                "name": "kostal",
                "host": "127.0.0.3",
                "port": 1502,
                "protocol": "modbus_tcp",
                "unit_id": 71,
                "modbus_byte_order": "CDAB",
                "sunspec_byte_order": "ABCD",
                "auth": {
                    "enabled": False,
                    "role": "plant_owner",
                    "web_access": {
                        "enabled": False,
                        "plant_owner_password": "",
                        "installer_service_code": "",
                        "installer_master_key": "",
                        "username": "",
                        "password": "",
                    },
                    "transport": {"uses_auth": False, "username": "", "password": ""},
                },
                "timeout_seconds": 5,
            },
        },
    }


@pytest.fixture
def config_file(tmp_path: Path, sample_config_dict: dict) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(sample_config_dict), encoding="utf-8")
    return config_path


@pytest.fixture
def store(tmp_path: Path) -> SQLiteStore:
    sqlite_path = tmp_path / "store.db"
    sqlite_store = SQLiteStore(str(sqlite_path))
    sqlite_store.initialize()
    return sqlite_store


@pytest.fixture
def app_instance(config_file: Path):
    app = create_app(str(config_file), start_polling=False)
    app.config["TESTING"] = True
    yield app


@pytest.fixture
def client(app_instance):
    return app_instance.test_client()


def insert_poll_result(
    store: SQLiteStore,
    *,
    device_name: str = "cfos",
    status: CollectorStatus = CollectorStatus.SUCCESS,
    success: bool = True,
    timestamp_utc: str = "2026-04-05T12:00:00+00:00",
    metric_name: str | None = "grid_power_w",
    metric_value: float = 123.0,
    source_type: str = "test",
) -> CollectorResult:
    measurements = []
    if metric_name is not None:
        measurements.append(
            MeasurementRecord(
                metric_name=metric_name,
                metric_value=metric_value,
                unit="W",
                source_type=source_type,
                raw_payload='{"mock": true}',
            )
        )
    result = CollectorResult(
        device_name=device_name,
        source_type=source_type,
        status=status,
        success=success,
        timestamp_utc=timestamp_utc,
        duration_ms=42,
        measurements=measurements,
        raw_payload='{"mock": true}',
        http_status=200 if success else None,
        error_message=None if success else "test failure",
        details={},
    )
    store.save_collector_result(result)
    return result
