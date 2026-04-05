from __future__ import annotations

from typing import Any

from collectors.base import CollectorStatus
from services.auth import summarize_cfos_auth, summarize_kostal_auth
from services.config_validation import ConfigValidationResult
from services.easee_cloud import build_easee_cloud_context
from storage.sqlite_store import SQLiteStore


def build_device_operations_view(
    device_name: str,
    device_config: dict[str, Any],
    store: SQLiteStore,
    config_validation: ConfigValidationResult,
    device_specs: dict[str, Any],
) -> dict[str, Any]:
    enabled = bool(device_config.get("enabled", False))
    host = device_config.get("host") or device_config.get("base_url") or "-"
    port = device_config.get("port")
    auth = device_config.get("auth", {}) or {}
    last_poll = store.get_recent_poll_events(device_name, limit=1)
    last_success = store.get_recent_success_poll(device_name)
    metrics_count = store.get_metric_count(device_name)
    last_data_read_utc = store.get_last_measurement_timestamp(device_name)
    raw_payload_available = store.has_raw_payload(device_name)
    recent_failures = store.count_recent_failures(device_name, limit=5)
    recording_summary = store.get_recording_summary(device_name)
    findings = config_validation.findings_for_scope(device_name)
    status = _derive_device_status(enabled, last_poll[0] if last_poll else None, findings, metrics_count)
    auth_summary = _auth_summary(device_name, auth, status, findings, device_specs, last_poll[0] if last_poll else None)
    easee_cloud_context = (
        build_easee_cloud_context(store, device_config)
        if device_name == "easee"
        else None
    )
    return {
        "name": device_name,
        "device_type": device_specs.get("device_type", device_name),
        "enabled": enabled,
        "host": host,
        "port": port,
        "auth_type": str(auth.get("type", "none")).lower(),
        "credentials_configured": _credentials_configured(auth),
        "credentials_expectation": _credential_expectation(auth),
        "auth_summary": auth_summary,
        "last_poll": last_poll[0] if last_poll else None,
        "last_successful_poll": last_success,
        "last_successful_data_read": last_data_read_utc,
        "last_error": (last_poll[0]["error_message"] if last_poll and last_poll[0]["error_message"] else None),
        "response_time_ms": (last_poll[0]["duration_ms"] if last_poll else None),
        "collector_status": status,
        "raw_payload_available": raw_payload_available,
        "normalized_metrics_available": metrics_count > 0,
        "normalized_metrics_count": metrics_count,
        "recent_failure_count": recent_failures,
        "recording_summary": recording_summary,
        "config_findings": findings,
        "mapping_note": _mapping_note(device_name),
        "settings_read_support_state": _support_state(device_specs, "settings_read_support_state"),
        "measurement_read_support_state": _support_state(device_specs, "measurement_read_support_state"),
        "mapping_state": device_specs.get("mapping_state", _support_state(device_specs, "measurement_read_support_state")),
        "protocol_summary": _protocol_summary(device_specs),
        "integration_context": easee_cloud_context,
        "specs": device_specs,
    }


def _derive_device_status(
    enabled: bool,
    last_poll: dict[str, Any] | None,
    findings: list[Any],
    metrics_count: int,
) -> str:
    if not enabled:
        return "disabled"
    if any(finding.severity == "error" for finding in findings):
        return "config_error"
    if any(
        finding.severity != "info"
        and ("credentials" in finding.message.lower() or "auth" in finding.key_path)
        for finding in findings
    ):
        if not last_poll:
            return "auth_missing"
    if not last_poll:
        return "never_polled"

    raw_status = last_poll["status"]
    if raw_status == CollectorStatus.SUCCESS.value:
        return "healthy" if metrics_count > 0 else "reachable"
    if raw_status == CollectorStatus.TIMEOUT.value:
        return "timeout"
    if raw_status == CollectorStatus.UNREACHABLE.value:
        return "unreachable"
    if raw_status == CollectorStatus.AUTH_FAILURE.value:
        return "auth_failed"
    if raw_status == CollectorStatus.PARSE_FAILURE.value:
        return "parsing_failed"
    if raw_status == CollectorStatus.MAPPING_NOT_IMPLEMENTED.value:
        return "mapping_incomplete"
    if raw_status == CollectorStatus.UNSUPPORTED_RESPONSE.value:
        return "unsupported_response"
    if raw_status == CollectorStatus.EMPTY_PAYLOAD.value:
        return "empty_payload"
    return "error"


def _credentials_configured(auth: dict[str, Any]) -> bool:
    auth_type = str(auth.get("type", "none")).lower()
    if auth_type == "basic":
        return bool(auth.get("username") and auth.get("password"))
    if auth_type == "bearer":
        return bool(auth.get("token"))
    return False


def _credential_expectation(auth: dict[str, Any]) -> str:
    auth_type = str(auth.get("type", "none")).lower()
    if auth_type == "none":
        return "unknown"
    if _credentials_configured(auth):
        return "configured"
    return "missing"


def _auth_summary(
    device_name: str,
    auth: dict[str, Any],
    collector_status: str,
    findings: list[Any],
    device_specs: dict[str, Any],
    last_poll: dict[str, Any] | None,
) -> dict[str, Any]:
    if device_name == "cfos":
        return summarize_cfos_auth(auth, collector_status, findings, (last_poll or {}).get("details"))
    if device_name == "kostal":
        return summarize_kostal_auth(auth, collector_status, str(device_specs.get("protocol", "modbus_tcp")))
    return {
        "enabled": bool(auth),
        "config_state": "unknown",
        "state": "unknown",
        "masked": {},
        "notes": [finding.message for finding in findings if "auth" in finding.key_path],
    }


def _mapping_note(device_name: str) -> str:
    if device_name == "cfos":
        return (
            "HTTP collector is sharper and can try configured candidate paths, but settings visibility and "
            "wallbox metric semantics are still only partially confirmed until real cFos payloads are validated."
        )
    if device_name == "easee":
        return (
            "Easee is currently treated as a cloud-linked wallbox behind cFos. "
            "The local IP may exist, but cFos evidence currently points to charger-ID-based cloud communication."
        )
    if device_name == "kostal":
        return (
            "Connectivity and byte-order handling are modeled for KOSTAL, but live register addresses and "
            "sign conventions are still only partially prepared, not fully verified."
        )
    return "Collector mapping completeness is unknown."


def _support_state(device_specs: dict[str, Any], field_name: str) -> str:
    protocols = (device_specs.get("protocols", {}) or {}).values()
    values = [protocol.get(field_name) for protocol in protocols if protocol.get("enabled")]
    if not values:
        return "not_configured"
    if "partial" in values:
        return "partial"
    if "prepared" in values:
        return "prepared"
    if "not_implemented" in values:
        return "not_implemented"
    return values[0]


def _protocol_summary(device_specs: dict[str, Any]) -> str:
    protocols = device_specs.get("protocols", {}) or {}
    enabled = [name for name, cfg in protocols.items() if cfg.get("enabled")]
    return ", ".join(enabled) if enabled else "none"
