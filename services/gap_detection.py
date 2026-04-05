from __future__ import annotations

from typing import Any

from storage.sqlite_store import SQLiteStore


def build_integration_gaps(
    store: SQLiteStore,
    device_operations: list[dict[str, Any]],
    device_specs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for device in device_operations:
        specs = device_specs.get(device["name"], {})
        if device["enabled"] and device["collector_status"] in {"reachable", "mapping_incomplete"}:
            if device["normalized_metrics_count"] == 0:
                gaps.append(
                    _gap(
                        device["name"],
                        "implementation_gap",
                        "warning",
                        "Device is reachable, but no normalized metrics are stored yet.",
                    )
                )
        if device["enabled"] and device["last_poll"] and not device["raw_payload_available"]:
            gaps.append(
                _gap(
                    device["name"],
                    "diagnostics_gap",
                    "warning",
                    "Poll events exist, but no raw payload is stored yet.",
                )
            )
        if device["name"] == "cfos":
            gaps.extend(_build_cfos_gaps(store, device, specs))
        if device["name"] == "kostal" and device["collector_status"] in {"reachable", "mapping_incomplete"}:
            gaps.append(
                _gap(
                    "kostal",
                    "mapping_gap",
                    "warning",
                    "KOSTAL connectivity is present, but Modbus/SunSpec register mapping remains incomplete.",
                )
            )
    return gaps


def _build_cfos_gaps(
    store: SQLiteStore,
    device: dict[str, Any],
    specs: dict[str, Any],
) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    cfos_protocols = (specs.get("protocols", {}) or {})
    http_protocol = cfos_protocols.get("http", {})
    mqtt_protocol = cfos_protocols.get("mqtt", {})
    latest_wallbox_power = store.get_latest_measurement_value("wallbox_power_w", device_name="cfos")

    if http_protocol.get("enabled") and device["collector_status"] in {"reachable", "healthy", "mapping_incomplete"}:
        if latest_wallbox_power is None:
            gaps.append(
                _gap(
                    "cfos",
                    "mapping_gap",
                    "warning",
                    "cFos responds, but no trusted wallbox power metric is mapped yet.",
                )
            )
    if http_protocol.get("enabled") and not mqtt_protocol.get("enabled"):
        gaps.append(
            _gap(
                "cfos",
                "implementation_gap",
                "info",
                "cFos HTTP is configured, but MQTT diagnostics are not configured yet.",
            )
        )
    if device["credentials_expectation"] == "configured" and device["collector_status"] == "auth_failed":
        gaps.append(
            _gap(
                "cfos",
                "issue",
                "error",
                "cFos credentials are configured but authentication is currently failing.",
            )
        )
    return gaps


def _gap(device_name: str, gap_type: str, severity: str, message: str) -> dict[str, str]:
    return {"device_name": device_name, "gap_type": gap_type, "severity": severity, "message": message}
