from __future__ import annotations

import socket
from datetime import datetime, timezone

from collectors.base import BaseCollector, CollectorResult, CollectorStatus
from services.kostal_mapping import build_kostal_mapping_profile


class KostalCollector(BaseCollector):
    def collect(self):  # type: ignore[override]
        started = datetime.now(timezone.utc)
        host = str(self.config.get("host", "")).strip()
        port = int(self.config.get("port", 1502))
        protocol = str(self.config.get("protocol", "modbus_tcp")).lower()
        mapping_profile = build_kostal_mapping_profile(self.config)
        if not host:
            return self._result(
                started=started,
                status=CollectorStatus.OTHER_ERROR,
                success=False,
                error_message="KOSTAL collector missing host in config.",
            )
        if protocol not in {"modbus_tcp", "sunspec_tcp"}:
            return self._result(
                started=started,
                status=CollectorStatus.UNSUPPORTED_RESPONSE,
                success=False,
                error_message=f"Unsupported KOSTAL protocol '{protocol}'.",
            )

        try:
            with socket.create_connection((host, port), timeout=self.build_timeout()[0]):
                return self._result(
                    started=started,
                    status=CollectorStatus.MAPPING_NOT_IMPLEMENTED,
                    success=False,
                    error_message=(
                        f"KOSTAL {protocol} endpoint is reachable on {host}:{port}, "
                        "but register/SunSpec mapping is not implemented yet."
                    ),
                    details={
                        "mapping_status": "connectivity_only",
                        "protocol": protocol,
                        "mapping_profile": mapping_profile,
                    },
                )
        except TimeoutError as exc:
            return self._result(
                started=started,
                status=CollectorStatus.TIMEOUT,
                success=False,
                error_message=f"KOSTAL TCP timeout on {host}:{port}: {exc}",
            )
        except OSError as exc:
            return self._result(
                started=started,
                status=CollectorStatus.UNREACHABLE,
                success=False,
                error_message=(
                    f"KOSTAL TCP connection failed on {host}:{port}: {exc}. "
                    "Check routing between subnets 192.168.50.x and 192.168.1.x."
                ),
                details={"protocol": protocol, "mapping_profile": mapping_profile},
            )
