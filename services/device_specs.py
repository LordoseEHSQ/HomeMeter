from __future__ import annotations

from typing import Any


def build_device_specs(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    devices = config.get("devices", {}) or {}
    return {
        "cfos": _build_cfos_specs(devices.get("cfos", {}) or {}),
        "easee": _build_easee_specs(devices.get("easee", {}) or {}),
        "kostal": _build_kostal_specs(devices.get("kostal", {}) or {}),
    }


def _build_cfos_specs(device_config: dict[str, Any]) -> dict[str, Any]:
    protocols = device_config.get("protocols", {}) or {}
    return {
        "device_type": "cfos_wallbox_booster",
        "preferred_protocols": device_config.get("preferred_protocols", ["http"]),
        "protocols": {
            "http": {
                "enabled": bool((protocols.get("http", {}) or {}).get("enabled", True)),
                "host": device_config.get("base_url"),
                "port": _extract_port(device_config.get("base_url"), fallback=80),
                "implemented": True,
                "status_path": device_config.get("status_path", "/"),
                "candidate_status_paths": device_config.get("candidate_status_paths", []),
                "settings_read_support_state": "partial",
                "measurement_read_support_state": "partial",
            },
            "mqtt": {
                "enabled": bool((protocols.get("mqtt", {}) or {}).get("enabled", False)),
                "host": (protocols.get("mqtt", {}) or {}).get("host"),
                "port": int((protocols.get("mqtt", {}) or {}).get("port", 1883) or 1883),
                "implemented": False,
                "settings_read_support_state": "not_implemented",
                "measurement_read_support_state": "prepared",
            },
            "modbus": {
                "enabled": bool((protocols.get("modbus", {}) or {}).get("enabled", False)),
                "host": (protocols.get("modbus", {}) or {}).get("host", _extract_host(device_config.get("base_url"))),
                "port": int((protocols.get("modbus", {}) or {}).get("port", 502) or 502),
                "implemented": False,
                "settings_read_support_state": "not_implemented",
                "measurement_read_support_state": "prepared",
            },
            "sunspec": {
                "enabled": bool((protocols.get("sunspec", {}) or {}).get("enabled", False)),
                "host": (protocols.get("sunspec", {}) or {}).get("host", _extract_host(device_config.get("base_url"))),
                "port": int((protocols.get("sunspec", {}) or {}).get("port", 1502) or 1502),
                "implemented": False,
                "settings_read_support_state": "not_implemented",
                "measurement_read_support_state": "prepared",
            },
        },
    }


def _build_easee_specs(device_config: dict[str, Any]) -> dict[str, Any]:
    return {
        "device_type": "easee_wallbox",
        "preferred_protocols": ["http"],
        "protocols": {
            "http": {
                "enabled": True,
                "host": device_config.get("base_url"),
                "port": _extract_port(device_config.get("base_url"), fallback=80),
                "implemented": True,
                "settings_read_support_state": "not_implemented",
                "measurement_read_support_state": "partial",
            }
        },
    }


def _build_kostal_specs(device_config: dict[str, Any]) -> dict[str, Any]:
    return {
        "device_type": "kostal_plenticore",
        "protocol": device_config.get("protocol", "modbus_tcp"),
        "port": int(device_config.get("port", 1502) or 1502),
        "unit_id": int(device_config.get("unit_id", 71) or 71),
        "modbus_byte_order": device_config.get("modbus_byte_order", "CDAB"),
        "sunspec_byte_order": device_config.get("sunspec_byte_order", "ABCD"),
        "mapping_state": "partial",
        "preferred_protocols": [device_config.get("protocol", "modbus_tcp")],
        "protocols": {
            "modbus_tcp": {
                "enabled": str(device_config.get("protocol", "modbus_tcp")).lower() == "modbus_tcp",
                "host": device_config.get("host"),
                "port": int(device_config.get("port", 1502) or 1502),
                "implemented": False,
                "settings_read_support_state": "not_implemented",
                "measurement_read_support_state": "prepared",
            },
            "sunspec_tcp": {
                "enabled": str(device_config.get("protocol", "modbus_tcp")).lower() == "sunspec_tcp",
                "host": device_config.get("host"),
                "port": int(device_config.get("port", 1502) or 1502),
                "implemented": False,
                "settings_read_support_state": "not_implemented",
                "measurement_read_support_state": "prepared",
            },
        },
    }


def _extract_host(base_url: Any) -> str | None:
    if not isinstance(base_url, str) or "://" not in base_url:
        return None
    return base_url.split("://", 1)[1].split("/", 1)[0].split(":", 1)[0]


def _extract_port(base_url: Any, fallback: int) -> int:
    if not isinstance(base_url, str) or "://" not in base_url:
        return fallback
    host_part = base_url.split("://", 1)[1].split("/", 1)[0]
    if ":" in host_part:
        try:
            return int(host_part.rsplit(":", 1)[1])
        except ValueError:
            return fallback
    return fallback
