from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any

import yaml
from flask import Flask

from analysis import PlausibilityEngine
from collectors import CfosCollector, EaseeCollector, KostalCollector
from collectors.base import BaseCollector
from services.config_validation import ConfigValidator
from services.cfos_protocols import CfosProtocolDiagnostics
from services.database_stats import DatabaseInspector
from services.device_specs import build_device_specs
from services.diagnostics import build_device_operations_view
from services.gap_detection import build_integration_gaps
from services.health import SystemHealthService
from services.time_monitor import TimeMonitor
from services.time_utils import format_cell_value, format_timestamp_for_display, load_time_settings
from storage import SQLiteStore
from web import register_routes

LOGGER = logging.getLogger(__name__)


class PollingManager:
    def __init__(
        self,
        collectors: list[BaseCollector],
        store: SQLiteStore,
        plausibility_engine: PlausibilityEngine,
        interval_seconds: int,
    ) -> None:
        self.collectors = collectors
        self.store = store
        self.plausibility_engine = plausibility_engine
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="polling-loop", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            cycle_started = time.time()
            self.run_cycle_once()
            elapsed = time.time() - cycle_started
            sleep_for = max(1.0, self.interval_seconds - elapsed)
            self._stop_event.wait(sleep_for)

    def run_cycle_once(self) -> list[Any]:
        alerts: list[Any] = []
        device_names = [collector.device_name for collector in self.collectors]
        cycle_timestamp: str | None = None
        for collector in self.collectors:
            LOGGER.info("Polling device %s", collector.device_name)
            result = collector.collect()
            cycle_timestamp = result.timestamp_utc
            self.store.save_collector_result(result)
            if result.success:
                LOGGER.info(
                    "Poll success for %s (%s, %sms, metrics=%s)",
                    result.device_name,
                    result.status.value,
                    result.duration_ms,
                    len(result.measurements),
                )
            else:
                LOGGER.warning(
                    "Poll failure for %s (%s, http=%s): %s",
                    result.device_name,
                    result.status.value,
                    result.http_status,
                    result.error_message,
                )
        if device_names and cycle_timestamp:
            alerts = self.plausibility_engine.run(
                device_names=device_names,
                current_timestamp_utc=cycle_timestamp,
            )
            for alert in alerts:
                LOGGER.warning(
                    "Alert generated [%s] %s: %s",
                    alert.rule_name,
                    alert.device_name,
                    alert.message,
                )
        return alerts

    def test_device(self, device_name: str) -> Any | None:
        for collector in self.collectors:
            if collector.device_name != device_name:
                continue
            result = collector.collect()
            self.store.save_collector_result(result)
            return result
        return None

    def replace_collectors(self, collectors: list[BaseCollector], interval_seconds: int) -> None:
        self.collectors = collectors
        self.interval_seconds = interval_seconds


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {config_path}. Copy config.example.yaml to config.yaml and adapt your device settings."
        )
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def create_collectors(config: dict[str, Any]) -> list[BaseCollector]:
    devices = config.get("devices", {}) or {}
    polling = config.get("polling", {}) or {}
    connect_timeout = float(polling.get("connect_timeout_seconds", 3))
    read_timeout = float(polling.get("read_timeout_seconds", 6))
    collectors: list[BaseCollector] = []

    cfos_config = devices.get("cfos", {}) or {}
    if cfos_config.get("enabled", False):
        collectors.append(
            CfosCollector(
                device_name=str(cfos_config.get("name", "cfos")),
                config=cfos_config,
                default_connect_timeout=connect_timeout,
                default_read_timeout=read_timeout,
            )
        )

    easee_config = devices.get("easee", {}) or {}
    if easee_config.get("enabled", False):
        collectors.append(
            EaseeCollector(
                device_name=str(easee_config.get("name", "easee")),
                config=easee_config,
                default_connect_timeout=connect_timeout,
                default_read_timeout=read_timeout,
            )
        )

    kostal_config = devices.get("kostal", {}) or {}
    if kostal_config.get("enabled", False):
        collectors.append(
            KostalCollector(
                device_name=str(kostal_config.get("name", "kostal")),
                config=kostal_config,
                default_connect_timeout=connect_timeout,
                default_read_timeout=read_timeout,
            )
        )

    return collectors


def sanitize_config(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if key.lower() in {"password", "token"} and item:
                sanitized[key] = "***masked***"
            else:
                sanitized[key] = sanitize_config(item)
        return sanitized
    if isinstance(value, list):
        return [sanitize_config(item) for item in value]
    return value


def create_app(config_path: str = "config.yaml", start_polling: bool = True) -> Flask:
    config = load_config(config_path)
    storage_config = config.get("storage", {}) or {}
    polling_config = config.get("polling", {}) or {}
    time_settings = load_time_settings(config)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logging.Formatter.converter = time.gmtime
    LOGGER.info("Configuration loaded from %s", config_path)

    store = SQLiteStore(str(storage_config.get("sqlite_path", "homemeter.db")))
    store.initialize()
    LOGGER.info("SQLite initialized at %s", storage_config.get("sqlite_path", "homemeter.db"))

    collectors = create_collectors(config)
    if not collectors:
        LOGGER.warning("No enabled collectors configured.")

    plausibility_engine = PlausibilityEngine(store=store)
    polling_manager = PollingManager(
        collectors=collectors,
        store=store,
        plausibility_engine=plausibility_engine,
        interval_seconds=int(polling_config.get("interval_seconds", 10)),
    )
    config_validator = ConfigValidator()
    db_inspector = DatabaseInspector(store)
    health_service = SystemHealthService(store=store)
    time_monitor = TimeMonitor(settings=time_settings)
    cfos_protocol_diagnostics = CfosProtocolDiagnostics()

    app = Flask(__name__)
    app.secret_key = "homemeter-local-dev"
    app.config["CONFIG"] = config
    app.config["CONFIG_PATH"] = config_path
    app.config["STORE"] = store
    app.config["DEVICE_NAMES"] = [collector.device_name for collector in collectors]
    app.config["POLLING_MANAGER"] = polling_manager
    app.config["CONFIG_VALIDATOR"] = config_validator
    app.config["DB_INSPECTOR"] = db_inspector
    app.config["HEALTH_SERVICE"] = health_service
    app.config["TIME_SETTINGS"] = time_settings
    app.config["TIME_MONITOR"] = time_monitor
    app.config["CFOS_PROTOCOL_DIAGNOSTICS"] = cfos_protocol_diagnostics
    app.config["SANITIZE_CONFIG"] = sanitize_config

    @app.template_filter("format_ts")
    def format_ts_filter(value: str | None) -> str:
        return format_timestamp_for_display(value, app.config["TIME_SETTINGS"])

    @app.template_filter("format_ts_detail")
    def format_ts_detail_filter(value: str | None) -> str:
        return format_timestamp_for_display(value, app.config["TIME_SETTINGS"], include_utc=True)

    @app.template_filter("format_cell")
    def format_cell_filter(payload: tuple[str, Any] | list[Any]) -> str:
        key, value = payload
        return format_cell_value(str(key), value, app.config["TIME_SETTINGS"])

    register_routes(app)

    def refresh_runtime_config() -> dict[str, Any]:
        fresh_config = load_config(app.config["CONFIG_PATH"])
        app.config["CONFIG"] = fresh_config
        app.config["TIME_SETTINGS"] = load_time_settings(fresh_config)
        app.config["TIME_MONITOR"] = TimeMonitor(app.config["TIME_SETTINGS"])
        app.config["CFOS_PROTOCOL_DIAGNOSTICS"] = CfosProtocolDiagnostics()
        refreshed_collectors = create_collectors(fresh_config)
        app.config["DEVICE_NAMES"] = [collector.device_name for collector in refreshed_collectors]
        app.config["POLLING_MANAGER"].replace_collectors(
            refreshed_collectors,
            int((fresh_config.get("polling", {}) or {}).get("interval_seconds", 10)),
        )
        return fresh_config

    def build_runtime_snapshot() -> dict[str, Any]:
        active_config = app.config["CONFIG"]
        validation_result = app.config["CONFIG_VALIDATOR"].validate(active_config)
        devices_config = (active_config.get("devices", {}) or {})
        device_specs = build_device_specs(active_config)
        known_device_names = ["cfos", "easee", "kostal"]
        operations = [
            build_device_operations_view(
                device_name=name,
                device_config=devices_config.get(name, {}) or {},
                store=store,
                config_validation=validation_result,
                device_specs=device_specs.get(name, {}),
            )
            for name in known_device_names
        ]
        db_report = app.config["DB_INSPECTOR"].build_report()
        system_health = app.config["HEALTH_SERVICE"].build_summary(operations, validation_result, db_report)
        integration_gaps = build_integration_gaps(store, operations, device_specs)
        cfos_protocol_statuses = app.config["CFOS_PROTOCOL_DIAGNOSTICS"].describe(
            devices_config.get("cfos", {}) or {},
            store,
        )
        return {
            "config_validation": validation_result,
            "device_operations": operations,
            "device_specs": device_specs,
            "database_report": db_report,
            "system_health": system_health,
            "integration_gaps": integration_gaps,
            "cfos_protocol_statuses": cfos_protocol_statuses,
            "time_settings": app.config["TIME_SETTINGS"],
            "time_status": app.config["TIME_MONITOR"].get_status(),
        }

    def build_validation_result():
        return app.config["CONFIG_VALIDATOR"].validate(app.config["CONFIG"])

    def build_device_specs_snapshot():
        return build_device_specs(app.config["CONFIG"])

    def build_device_operations_snapshot(validation_result, device_specs):
        devices_config = (app.config["CONFIG"].get("devices", {}) or {})
        known_device_names = ["cfos", "easee", "kostal"]
        return [
            build_device_operations_view(
                device_name=name,
                device_config=devices_config.get(name, {}) or {},
                store=store,
                config_validation=validation_result,
                device_specs=device_specs.get(name, {}),
            )
            for name in known_device_names
        ]

    app.config["REFRESH_RUNTIME_CONFIG"] = refresh_runtime_config
    app.config["BUILD_RUNTIME_SNAPSHOT"] = build_runtime_snapshot
    app.config["BUILD_VALIDATION_RESULT"] = build_validation_result
    app.config["BUILD_DEVICE_SPECS"] = build_device_specs_snapshot
    app.config["BUILD_DEVICE_OPERATIONS"] = build_device_operations_snapshot

    if start_polling:
        polling_manager.start()
        LOGGER.info("Background polling loop started.")
    return app
