from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def update_config_from_form(config: dict[str, Any], scope: str, form: dict[str, str]) -> dict[str, Any]:
    updated = yaml.safe_load(yaml.safe_dump(config)) or {}
    devices = updated.setdefault("devices", {})
    if scope == "cfos":
        cfos = devices.setdefault("cfos", {})
        auth = cfos.setdefault("auth", {})
        cfos["base_url"] = form.get("base_url", cfos.get("base_url", ""))
        cfos["status_path"] = form.get("status_path", cfos.get("status_path", "/")) or "/"
        cfos["timeout_seconds"] = _to_number(form.get("timeout_seconds"), cfos.get("timeout_seconds", 5))
        cfos["candidate_status_paths"] = _to_lines(form.get("candidate_status_paths"), cfos.get("candidate_status_paths", []))
        auth["enabled"] = _to_bool(form.get("auth_enabled"), auth.get("enabled", True))
        auth["type"] = form.get("auth_type", auth.get("type", "basic")) or "basic"
        auth["credential_source"] = form.get("credential_source", auth.get("credential_source", "custom")) or "custom"
        auth["default_username"] = form.get("default_username", auth.get("default_username", "admin")) or "admin"
        auth["default_password_variants"] = _to_lines(
            form.get("default_password_variants"),
            auth.get("default_password_variants", ["", "1234abcd"]),
            preserve_empty=True,
        )
        if form.get("username", "") != "":
            auth["username"] = form.get("username", "")
        if "password" in form and form.get("password", "") != "":
            auth["password"] = form.get("password", "")
        elif auth.get("credential_source") == "default_auto":
            auth["password"] = ""
        auth["token"] = auth.get("token", "")
    elif scope == "kostal":
        kostal = devices.setdefault("kostal", {})
        auth = kostal.setdefault("auth", {})
        web_access = auth.setdefault("web_access", {})
        transport = auth.setdefault("transport", {})
        kostal["host"] = form.get("host", kostal.get("host", ""))
        kostal["port"] = int(_to_number(form.get("port"), kostal.get("port", 1502)))
        kostal["protocol"] = form.get("protocol", kostal.get("protocol", "modbus_tcp")) or "modbus_tcp"
        kostal["unit_id"] = int(_to_number(form.get("unit_id"), kostal.get("unit_id", 71)))
        kostal["modbus_byte_order"] = form.get("modbus_byte_order", kostal.get("modbus_byte_order", "CDAB")) or "CDAB"
        kostal["sunspec_byte_order"] = form.get("sunspec_byte_order", kostal.get("sunspec_byte_order", "ABCD")) or "ABCD"
        kostal["timeout_seconds"] = _to_number(form.get("timeout_seconds"), kostal.get("timeout_seconds", 5))
        auth["enabled"] = _to_bool(form.get("auth_enabled"), auth.get("enabled", False))
        auth["role"] = form.get("role", auth.get("role", "plant_owner")) or "plant_owner"
        web_access["enabled"] = _to_bool(form.get("web_access_enabled"), web_access.get("enabled", auth.get("enabled", False)))
        transport["uses_auth"] = _to_bool(form.get("transport_uses_auth"), transport.get("uses_auth", False))
        for key in ("username", "password", "plant_owner_password", "installer_service_code", "installer_master_key"):
            if key in form and form.get(key, "") != "":
                web_access[key] = form.get(key, "")
        if "transport_username" in form and form.get("transport_username", "") != "":
            transport["username"] = form.get("transport_username", "")
        if "transport_password" in form and form.get("transport_password", "") != "":
            transport["password"] = form.get("transport_password", "")
    elif scope == "time":
        time_config = updated.setdefault("time", {})
        time_config["display_timezone"] = form.get("display_timezone", time_config.get("display_timezone", "Europe/Berlin"))
        time_config["display_format"] = form.get("display_format", time_config.get("display_format", "%d.%m.%Y %H:%M:%S"))
    elif scope == "analytics":
        analytics = updated.setdefault("analytics", {})
        analytics["default_window"] = form.get("default_window", analytics.get("default_window", "24h")) or "24h"
        analytics["chart_refresh_seconds"] = int(_to_number(form.get("chart_refresh_seconds"), analytics.get("chart_refresh_seconds", 30)))
        analytics["rollup_retention_days"] = int(_to_number(form.get("rollup_retention_days"), analytics.get("rollup_retention_days", 180)))
    else:
        raise ValueError(f"Unsupported config scope: {scope}")
    return updated


def save_config(path: str, config: dict[str, Any]) -> None:
    target = Path(path)
    target.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=False), encoding="utf-8")


def _to_bool(value: str | None, fallback: bool) -> bool:
    if value is None:
        return fallback
    return str(value).lower() in {"1", "true", "yes", "on"}


def _to_number(value: str | None, fallback: Any) -> float | int:
    if value in {None, ""}:
        return fallback
    try:
        number = float(value)
    except ValueError:
        return fallback
    return int(number) if float(number).is_integer() else number


def _to_lines(value: str | None, fallback: list[str], preserve_empty: bool = False) -> list[str]:
    if value is None:
        return fallback
    lines = [line.strip() for line in str(value).replace(",", "\n").splitlines()]
    cleaned = [line for line in lines if line or preserve_empty]
    return cleaned or fallback
