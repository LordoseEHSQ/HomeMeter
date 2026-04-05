from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ConfigFinding:
    severity: str
    scope: str
    message: str
    key_path: str


@dataclass(slots=True)
class ConfigValidationResult:
    is_valid: bool
    findings: list[ConfigFinding] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def status(self) -> str:
        if self.error_count:
            return "error"
        if self.warning_count:
            return "warning"
        return "healthy"

    def findings_for_scope(self, scope: str) -> list[ConfigFinding]:
        return [finding for finding in self.findings if finding.scope == scope]


class ConfigValidator:
    def validate(self, config: dict[str, Any]) -> ConfigValidationResult:
        findings: list[ConfigFinding] = []
        if not config:
            findings.append(
                ConfigFinding(
                    severity="error",
                    scope="system",
                    key_path="config",
                    message="Configuration is empty or could not be parsed.",
                )
            )
            return ConfigValidationResult(is_valid=False, findings=findings)

        app = config.get("app")
        polling = config.get("polling")
        storage = config.get("storage")
        devices = config.get("devices")
        time_config = config.get("time", {}) or {}

        if not isinstance(app, dict):
            findings.append(self._finding("error", "system", "app", "Missing top-level 'app' section."))
        if not isinstance(polling, dict):
            findings.append(self._finding("error", "system", "polling", "Missing top-level 'polling' section."))
        if not isinstance(storage, dict):
            findings.append(self._finding("error", "system", "storage", "Missing top-level 'storage' section."))
        if not isinstance(devices, dict):
            findings.append(self._finding("error", "system", "devices", "Missing top-level 'devices' section."))
            return ConfigValidationResult(is_valid=False, findings=findings)

        interval = (polling or {}).get("interval_seconds")
        if interval is None or not self._is_positive_number(interval):
            findings.append(
                self._finding("error", "system", "polling.interval_seconds", "Polling interval must be a positive number.")
            )
        elif float(interval) < 3:
            findings.append(
                self._finding(
                    "warning",
                    "system",
                    "polling.interval_seconds",
                    "Polling interval below 3 seconds is aggressive for a diagnostics-first local app.",
                )
            )

        connect_timeout = (polling or {}).get("connect_timeout_seconds")
        read_timeout = (polling or {}).get("read_timeout_seconds")
        for key, value in {
            "polling.connect_timeout_seconds": connect_timeout,
            "polling.read_timeout_seconds": read_timeout,
        }.items():
            if value is not None and not self._is_positive_number(value):
                findings.append(self._finding("error", "system", key, f"{key} must be a positive number."))

        sqlite_path = (storage or {}).get("sqlite_path")
        if not sqlite_path:
            findings.append(
                self._finding("error", "system", "storage.sqlite_path", "SQLite path is missing.")
            )

        display_timezone = str(time_config.get("display_timezone", "Europe/Berlin"))
        display_format = str(time_config.get("display_format", "%d.%m.%Y %H:%M:%S"))
        if not display_timezone:
            findings.append(
                self._finding("warning", "time", "time.display_timezone", "Display timezone is missing; defaulting to Europe/Berlin.")
            )
        if not display_format:
            findings.append(
                self._finding("warning", "time", "time.display_format", "Display format is missing; defaulting to DD.MM.YYYY HH:MM:SS.")
            )
        ntp_config = time_config.get("ntp", {}) or {}
        if ntp_config and not isinstance(ntp_config, dict):
            findings.append(self._finding("error", "time", "time.ntp", "time.ntp must be a mapping."))
        else:
            self._validate_time_settings(ntp_config, findings)

        known_devices = ("cfos", "easee", "kostal")
        for device_key in known_devices:
            device_config = devices.get(device_key, {})
            if not isinstance(device_config, dict):
                findings.append(
                    self._finding("error", device_key, f"devices.{device_key}", "Device config must be a mapping.")
                )
                continue
            findings.extend(self._validate_device(device_key, device_config))

        return ConfigValidationResult(is_valid=not any(f.severity == "error" for f in findings), findings=findings)

    def _validate_device(self, device_key: str, device_config: dict[str, Any]) -> list[ConfigFinding]:
        findings: list[ConfigFinding] = []
        enabled = bool(device_config.get("enabled", False))
        auth = device_config.get("auth", {}) or {}
        timeout = device_config.get("timeout_seconds")
        if timeout is not None and not self._is_positive_number(timeout):
            findings.append(
                self._finding("error", device_key, f"devices.{device_key}.timeout_seconds", "Timeout must be positive.")
            )

        if not enabled:
            findings.append(
                self._finding("info", device_key, f"devices.{device_key}.enabled", "Device is disabled by configuration.")
            )
            return findings

        if device_key in {"cfos", "easee"}:
            if not device_config.get("base_url"):
                findings.append(
                    self._finding("error", device_key, f"devices.{device_key}.base_url", "Enabled device is missing base_url.")
                )
        if device_key == "kostal":
            if not device_config.get("host"):
                findings.append(
                    self._finding("error", device_key, "devices.kostal.host", "Enabled KOSTAL device is missing host.")
                )
            port = device_config.get("port")
            if port is not None and (not isinstance(port, int) or port <= 0):
                findings.append(self._finding("error", device_key, "devices.kostal.port", "Port must be a positive integer."))
            if str(device_config.get("protocol", "modbus_tcp")).lower() not in {"modbus_tcp", "sunspec_tcp"}:
                findings.append(
                    self._finding(
                        "error",
                        device_key,
                        "devices.kostal.protocol",
                        "KOSTAL protocol must be modbus_tcp or sunspec_tcp.",
                    )
                )
            if not isinstance(device_config.get("unit_id", 71), int):
                findings.append(
                    self._finding("warning", device_key, "devices.kostal.unit_id", "KOSTAL unit_id should be an integer.")
                )
            if str(device_config.get("modbus_byte_order", "CDAB")).upper() not in {"ABCD", "BADC", "CDAB", "DCBA"}:
                findings.append(
                    self._finding(
                        "warning",
                        device_key,
                        "devices.kostal.modbus_byte_order",
                        "Unexpected KOSTAL Modbus byte order value.",
                    )
                )
            if str(device_config.get("sunspec_byte_order", "ABCD")).upper() not in {"ABCD", "BADC", "CDAB", "DCBA"}:
                findings.append(
                    self._finding(
                        "warning",
                        device_key,
                        "devices.kostal.sunspec_byte_order",
                        "Unexpected KOSTAL SunSpec byte order value.",
                    )
                )
        if device_key == "cfos":
            self._validate_cfos_protocols(device_config, findings)

        auth_type = str(auth.get("type", "none")).lower()
        if auth_type not in {"none", "basic", "bearer"}:
            findings.append(
                self._finding(
                    "warning",
                    device_key,
                    f"devices.{device_key}.auth.type",
                    f"Unknown auth type '{auth_type}'. Expected none, basic or bearer.",
                )
            )

        if auth_type == "basic":
            if not auth.get("username") or not auth.get("password"):
                findings.append(
                    self._finding(
                        "warning",
                        device_key,
                        f"devices.{device_key}.auth",
                        "Basic auth selected but username or password is missing.",
                    )
                )
        elif auth_type == "bearer" and not auth.get("token"):
            findings.append(
                self._finding(
                    "warning",
                    device_key,
                    f"devices.{device_key}.auth.token",
                    "Bearer auth selected but token is missing.",
                )
            )
        elif auth_type == "none":
            findings.append(
                self._finding(
                    "info",
                    device_key,
                    f"devices.{device_key}.auth.type",
                    "No credentials configured. This may be correct, or auth may still be required by the real device.",
                )
            )

        if not device_config.get("status_path"):
            findings.append(
                self._finding(
                    "warning",
                    device_key,
                    f"devices.{device_key}.status_path",
                    "Status path is empty; collector will default to the device root path.",
                )
            )

        return findings

    def _validate_cfos_protocols(
        self,
        device_config: dict[str, Any],
        findings: list[ConfigFinding],
    ) -> None:
        protocols = device_config.get("protocols", {}) or {}
        if protocols and not isinstance(protocols, dict):
            findings.append(
                self._finding("error", "cfos", "devices.cfos.protocols", "cFos protocols must be a mapping.")
            )
            return
        for surface in ("http", "mqtt", "modbus", "sunspec"):
            protocol_cfg = protocols.get(surface, {}) or {}
            enabled = bool(protocol_cfg.get("enabled", surface == "http"))
            if not enabled:
                continue
            if surface == "http":
                if not device_config.get("base_url"):
                    findings.append(
                        self._finding("error", "cfos", "devices.cfos.base_url", "cFos HTTP is enabled but base_url is missing.")
                    )
                candidate_paths = device_config.get("candidate_status_paths")
                if candidate_paths is not None and not isinstance(candidate_paths, list):
                    findings.append(
                        self._finding(
                            "warning",
                            "cfos",
                            "devices.cfos.candidate_status_paths",
                            "cFos candidate_status_paths should be a list of HTTP paths.",
                        )
                    )
            else:
                if not protocol_cfg.get("host"):
                    findings.append(
                        self._finding(
                            "warning",
                            "cfos",
                            f"devices.cfos.protocols.{surface}.host",
                            f"cFos {surface} diagnostics are enabled but host is missing.",
                        )
                    )
                port = protocol_cfg.get("port")
                if port is not None and (not isinstance(port, int) or port <= 0):
                    findings.append(
                        self._finding(
                            "error",
                            "cfos",
                            f"devices.cfos.protocols.{surface}.port",
                            f"cFos {surface} port must be a positive integer.",
                        )
                    )

    def _validate_time_settings(self, ntp_config: dict[str, Any], findings: list[ConfigFinding]) -> None:
        enabled = bool(ntp_config.get("enabled", False))
        servers = ntp_config.get("servers", []) or []
        timeout_seconds = ntp_config.get("timeout_seconds")
        drift_warning_seconds = ntp_config.get("drift_warning_seconds")
        if timeout_seconds is not None and not self._is_positive_number(timeout_seconds):
            findings.append(
                self._finding("error", "time", "time.ntp.timeout_seconds", "NTP timeout must be a positive number.")
            )
        if drift_warning_seconds is not None and not self._is_positive_number(drift_warning_seconds):
            findings.append(
                self._finding("error", "time", "time.ntp.drift_warning_seconds", "Drift warning threshold must be positive.")
            )
        if enabled and not servers:
            findings.append(
                self._finding(
                    "warning",
                    "time",
                    "time.ntp.servers",
                    "Reference time checking is enabled but no NTP servers are configured.",
                )
            )

    def _finding(self, severity: str, scope: str, key_path: str, message: str) -> ConfigFinding:
        return ConfigFinding(severity=severity, scope=scope, key_path=key_path, message=message)

    def _is_positive_number(self, value: Any) -> bool:
        try:
            return float(value) > 0
        except (TypeError, ValueError):
            return False
