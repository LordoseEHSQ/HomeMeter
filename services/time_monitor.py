from __future__ import annotations

import socket
import struct
from datetime import datetime, timezone
from typing import Any

from services.time_utils import TimeSettings, utc_now_storage

NTP_EPOCH_OFFSET = 2208988800


class TimeMonitor:
    def __init__(self, settings: TimeSettings) -> None:
        self.settings = settings
        self._status = {
            "enabled": settings.ntp_enabled,
            "servers": settings.ntp_servers or [],
            "timeout_seconds": settings.ntp_timeout_seconds,
            "drift_warning_seconds": settings.drift_warning_seconds,
            "last_attempt_utc": None,
            "last_success_utc": None,
            "last_error": None,
            "last_drift_seconds": None,
            "within_tolerance": None,
            "checked_server": None,
            "status": "disabled" if not settings.ntp_enabled else "not_checked",
            "implementation_note": (
                "Reference time checking queries configured NTP servers and estimates drift. "
                "It does not change the operating system clock."
            ),
        }

    def get_status(self) -> dict[str, Any]:
        return dict(self._status)

    def run_check(self) -> dict[str, Any]:
        self._status["last_attempt_utc"] = utc_now_storage()
        self._status["last_error"] = None
        self._status["checked_server"] = None
        if not self.settings.ntp_enabled:
            self._status["status"] = "disabled"
            return self.get_status()
        if not self.settings.ntp_servers:
            self._status["status"] = "config_error"
            self._status["last_error"] = "NTP/reference checking is enabled but no servers are configured."
            return self.get_status()

        last_exception: Exception | None = None
        for server in self.settings.ntp_servers:
            try:
                reference_time = self._query_ntp_server(server, timeout=self.settings.ntp_timeout_seconds)
                drift_seconds = abs(
                    (datetime.now(timezone.utc) - reference_time).total_seconds()
                )
                self._status["last_success_utc"] = utc_now_storage()
                self._status["last_drift_seconds"] = round(drift_seconds, 3)
                self._status["within_tolerance"] = drift_seconds <= self.settings.drift_warning_seconds
                self._status["checked_server"] = server
                self._status["status"] = "healthy" if self._status["within_tolerance"] else "warning"
                return self.get_status()
            except Exception as exc:
                last_exception = exc

        self._status["status"] = "error"
        self._status["last_error"] = str(last_exception) if last_exception else "Unknown NTP check failure."
        self._status["within_tolerance"] = None
        return self.get_status()

    def _query_ntp_server(self, host: str, timeout: float) -> datetime:
        payload = b"\x1b" + 47 * b"\0"
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(timeout)
            sock.sendto(payload, (host, 123))
            data, _ = sock.recvfrom(48)
        if len(data) < 48:
            raise ValueError(f"Incomplete NTP response from {host}.")
        transmit_timestamp = struct.unpack("!12I", data)[10]
        unix_seconds = transmit_timestamp - NTP_EPOCH_OFFSET
        return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
