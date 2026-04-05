from __future__ import annotations

import json
from typing import Any

from storage.sqlite_store import SQLiteStore


def build_easee_cloud_context(store: SQLiteStore, easee_config: dict[str, Any]) -> dict[str, Any]:
    payload_row = store.get_latest_raw_payload("cfos")
    base_context = {
        "transport_mode": "unknown",
        "charger_id": None,
        "cfos_device_id": None,
        "cfos_device_type": None,
        "cfos_address": None,
        "local_ip": _extract_local_ip(easee_config),
        "local_ip_role": "secondary_or_unknown",
        "operator_hint": None,
        "cloud_auth_state": "unknown",
        "cloud_auth_error": None,
        "communication_summary": "No cFos payload evidence available yet for Easee cloud integration.",
    }
    if not payload_row or not payload_row.get("raw_payload"):
        return base_context
    try:
        parsed = json.loads(payload_row["raw_payload"])
    except json.JSONDecodeError:
        base_context["communication_summary"] = "Latest cFos raw payload could not be parsed for Easee cloud diagnostics."
        return base_context

    for device in parsed.get("devices", []) or []:
        if not isinstance(device, dict):
            continue
        if str(device.get("dev_type", "")).lower() != "evse_easee":
            continue
        address = str(device.get("address", "") or "")
        last_error = str(device.get("last_error", "") or "")
        cloud_auth_state = "ok"
        if "invalid refresh token" in last_error.lower():
            cloud_auth_state = "token_refresh_failed"
        elif last_error:
            cloud_auth_state = "error"
        return {
            "transport_mode": "easee_cloud_via_cfos",
            "charger_id": address or None,
            "cfos_device_id": device.get("dev_id"),
            "cfos_device_type": device.get("dev_type"),
            "cfos_address": address or None,
            "local_ip": _extract_local_ip(easee_config),
            "local_ip_role": "not_primary_transport",
            "operator_hint": "Select 'cFos eMobility' as operator in the Easee site configuration.",
            "cloud_auth_state": cloud_auth_state,
            "cloud_auth_error": last_error or None,
            "communication_summary": (
                "cFos currently addresses the Easee by charger ID via the Easee cloud integration, "
                "not by the local IP as the primary transport."
            ),
        }
    return base_context


def _extract_local_ip(easee_config: dict[str, Any]) -> str | None:
    base_url = str(easee_config.get("base_url", "") or "")
    if "://" not in base_url:
        return None
    host = base_url.split("://", 1)[1].split("/", 1)[0]
    return host.split(":", 1)[0]
