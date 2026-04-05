from __future__ import annotations

import socket
from typing import Any

from services.time_utils import utc_now_storage
from storage.sqlite_store import SQLiteStore


class CfosProtocolDiagnostics:
    def __init__(self) -> None:
        self.runtime_status: dict[str, dict[str, Any]] = {}

    def describe(self, cfos_config: dict[str, Any], store: SQLiteStore) -> list[dict[str, Any]]:
        protocols = cfos_config.get("protocols", {}) or {}
        http_cfg = protocols.get("http", {}) or {}
        mqtt_cfg = protocols.get("mqtt", {}) or {}
        modbus_cfg = protocols.get("modbus", {}) or {}
        sunspec_cfg = protocols.get("sunspec", {}) or {}
        last_poll = store.get_recent_poll_events("cfos", limit=1)
        last_success = store.get_recent_success_poll("cfos")

        statuses = [
            self._http_status(cfos_config, http_cfg, last_poll[0] if last_poll else None, last_success),
            self._generic_surface_status("mqtt", mqtt_cfg, default_port=1883, implemented=False),
            self._generic_surface_status("modbus", modbus_cfg, default_port=502, implemented=False),
            self._generic_surface_status("sunspec", sunspec_cfg, default_port=1502, implemented=False),
        ]
        return [self.runtime_status.get(item["surface"], item) for item in statuses]

    def test_surface(self, surface: str, cfos_config: dict[str, Any], store: SQLiteStore) -> dict[str, Any]:
        protocols = cfos_config.get("protocols", {}) or {}
        if surface == "http":
            last_poll = store.get_recent_poll_events("cfos", limit=1)
            last_success = store.get_recent_success_poll("cfos")
            status = self._http_status(
                cfos_config,
                protocols.get("http", {}) or {},
                last_poll[0] if last_poll else None,
                last_success,
            )
        else:
            protocol_cfg = protocols.get(surface, {}) or {}
            status = self._generic_surface_status(
                surface=surface,
                protocol_cfg=protocol_cfg,
                default_port={"mqtt": 1883, "modbus": 502, "sunspec": 1502}.get(surface, 0),
                implemented=False,
                do_probe=True,
            )
        self.runtime_status[surface] = status
        return status

    def _http_status(
        self,
        cfos_config: dict[str, Any],
        protocol_cfg: dict[str, Any],
        last_poll: dict[str, Any] | None,
        last_success: dict[str, Any] | None,
    ) -> dict[str, Any]:
        auth = cfos_config.get("auth", {}) or {}
        credentials_configured = bool(auth.get("username") and auth.get("password"))
        status = "configured"
        if last_poll:
            status = last_poll["status"]
        return {
            "surface": "http",
            "enabled": bool(protocol_cfg.get("enabled", True)),
            "configured": bool(cfos_config.get("base_url")),
            "host": cfos_config.get("base_url"),
            "port": _extract_port(cfos_config.get("base_url"), 80),
            "implemented": True,
            "reachable": status in {"success", "mapping_not_implemented", "parse_failure", "empty_payload"},
            "auth_configured": credentials_configured,
            "auth_status": "configured" if credentials_configured else "unknown",
            "data_status": "available" if last_success else "unknown",
            "settings_read_support_state": "partial",
            "measurement_read_support_state": "partial",
            "implementation_state": "partial",
            "last_attempt_utc": last_poll["timestamp_utc"] if last_poll else None,
            "last_success_utc": last_success["timestamp_utc"] if last_success else None,
            "last_error": last_poll["error_message"] if last_poll else None,
        }

    def _generic_surface_status(
        self,
        surface: str,
        protocol_cfg: dict[str, Any],
        default_port: int,
        implemented: bool,
        do_probe: bool = False,
    ) -> dict[str, Any]:
        host = protocol_cfg.get("host")
        port = int(protocol_cfg.get("port", default_port) or default_port)
        enabled = bool(protocol_cfg.get("enabled", False))
        configured = enabled and bool(host)
        probe_ok = None
        error = None
        if do_probe and configured:
            try:
                with socket.create_connection((str(host), port), timeout=float(protocol_cfg.get("timeout_seconds", 2))):
                    probe_ok = True
            except OSError as exc:
                probe_ok = False
                error = str(exc)
        return {
            "surface": surface,
            "enabled": enabled,
            "configured": configured,
            "host": host or "-",
            "port": port,
            "implemented": implemented,
            "reachable": probe_ok,
            "auth_configured": False,
            "auth_status": "not_implemented",
            "data_status": "not_implemented",
            "settings_read_support_state": "not_implemented",
            "measurement_read_support_state": "prepared" if configured else "not_configured",
            "implementation_state": "prepared" if configured else "not_configured",
            "last_attempt_utc": utc_now_storage() if do_probe and configured else None,
            "last_success_utc": utc_now_storage() if probe_ok else None,
            "last_error": error,
        }


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
