from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import requests


class CollectorStatus(str, Enum):
    SUCCESS = "success"
    UNREACHABLE = "unreachable"
    TIMEOUT = "timeout"
    AUTH_FAILURE = "auth_failure"
    PARSE_FAILURE = "parse_failure"
    MAPPING_NOT_IMPLEMENTED = "mapping_not_implemented"
    EMPTY_PAYLOAD = "empty_payload"
    UNSUPPORTED_RESPONSE = "unsupported_response"
    OTHER_ERROR = "other_error"


@dataclass(slots=True)
class MeasurementRecord:
    metric_name: str
    metric_value: float
    unit: str | None = None
    source_type: str = "normalized"
    raw_payload: str | None = None


@dataclass(slots=True)
class CollectorResult:
    device_name: str
    source_type: str
    status: CollectorStatus
    success: bool
    timestamp_utc: str
    duration_ms: int
    measurements: list[MeasurementRecord] = field(default_factory=list)
    raw_payload: str | None = None
    http_status: int | None = None
    error_message: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


class BaseCollector:
    source_type = "generic_http"

    def __init__(
        self,
        device_name: str,
        config: dict[str, Any],
        default_connect_timeout: float,
        default_read_timeout: float,
    ) -> None:
        self.device_name = device_name
        self.config = config
        self.default_connect_timeout = default_connect_timeout
        self.default_read_timeout = default_read_timeout

    def collect(self) -> CollectorResult:
        started = datetime.now(timezone.utc)
        try:
            response = self.perform_request()
            raw_payload = self.extract_payload(response)
            if raw_payload is None or raw_payload == "":
                return self._result(
                    started=started,
                    status=CollectorStatus.EMPTY_PAYLOAD,
                    success=False,
                    raw_payload=raw_payload,
                    http_status=response.status_code if response is not None else None,
                    error_message="Received empty payload from device.",
                )
            parsed = self.parse_payload(raw_payload, response)
            measurements, details = self.normalize_payload(parsed, raw_payload)
            return self._result(
                started=started,
                status=CollectorStatus.SUCCESS,
                success=True,
                raw_payload=raw_payload,
                http_status=response.status_code if response is not None else None,
                measurements=measurements,
                details=details,
            )
        except requests.exceptions.Timeout as exc:
            return self._result(
                started=started,
                status=CollectorStatus.TIMEOUT,
                success=False,
                error_message=f"Timeout while contacting device: {exc}",
            )
        except requests.exceptions.ConnectionError as exc:
            return self._result(
                started=started,
                status=CollectorStatus.UNREACHABLE,
                success=False,
                error_message=f"Connection failed: {exc}",
            )
        except requests.exceptions.HTTPError as exc:
            http_status = exc.response.status_code if exc.response is not None else None
            status = CollectorStatus.AUTH_FAILURE if http_status in {401, 403} else CollectorStatus.OTHER_ERROR
            return self._result(
                started=started,
                status=status,
                success=False,
                http_status=http_status,
                error_message=f"HTTP error from device: {exc}",
            )
        except NotImplementedError as exc:
            return self._result(
                started=started,
                status=CollectorStatus.MAPPING_NOT_IMPLEMENTED,
                success=False,
                error_message=str(exc),
            )
        except ValueError as exc:
            return self._result(
                started=started,
                status=CollectorStatus.PARSE_FAILURE,
                success=False,
                error_message=str(exc),
            )
        except Exception as exc:
            return self._result(
                started=started,
                status=CollectorStatus.OTHER_ERROR,
                success=False,
                error_message=f"Unhandled collector error: {exc}",
            )

    def perform_request(self) -> requests.Response:
        raise NotImplementedError

    def extract_payload(self, response: requests.Response | None) -> str | None:
        if response is None:
            return None
        return response.text

    def parse_payload(self, raw_payload: str, response: requests.Response | None) -> Any:
        raise NotImplementedError

    def normalize_payload(
        self, parsed_payload: Any, raw_payload: str
    ) -> tuple[list[MeasurementRecord], dict[str, Any]]:
        raise NotImplementedError

    def build_timeout(self) -> tuple[float, float]:
        timeout = float(self.config.get("timeout_seconds", 0) or 0)
        if timeout > 0:
            return (timeout, timeout)
        return (self.default_connect_timeout, self.default_read_timeout)

    def get_session(self) -> requests.Session:
        session = requests.Session()
        session.trust_env = False
        return session

    def _result(
        self,
        *,
        started: datetime,
        status: CollectorStatus,
        success: bool,
        raw_payload: str | None = None,
        http_status: int | None = None,
        error_message: str | None = None,
        measurements: list[MeasurementRecord] | None = None,
        details: dict[str, Any] | None = None,
    ) -> CollectorResult:
        finished = datetime.now(timezone.utc)
        duration_ms = int((finished - started).total_seconds() * 1000)
        return CollectorResult(
            device_name=self.device_name,
            source_type=self.source_type,
            status=status,
            success=success,
            timestamp_utc=finished.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_ms=duration_ms,
            measurements=measurements or [],
            raw_payload=raw_payload,
            http_status=http_status,
            error_message=error_message,
            details=details or {},
        )
