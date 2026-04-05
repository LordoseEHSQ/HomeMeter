from __future__ import annotations

import json
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from collectors.base import BaseCollector, CollectorStatus, MeasurementRecord


class EaseeCollector(BaseCollector):
    source_type = "easee_http"

    def perform_request(self) -> requests.Response:
        base_url = str(self.config.get("base_url", "")).rstrip("/")
        status_path = str(self.config.get("status_path", "/"))
        if not base_url:
            raise ValueError("Easee collector missing base_url in config.")

        auth_config = self.config.get("auth", {}) or {}
        auth_type = str(auth_config.get("type", "none")).lower()
        headers: dict[str, str] = {}
        auth = None
        if auth_type == "basic":
            auth = HTTPBasicAuth(
                str(auth_config.get("username", "")),
                str(auth_config.get("password", "")),
            )
        elif auth_type == "bearer" and auth_config.get("token"):
            headers["Authorization"] = f"Bearer {auth_config['token']}"

        url = f"{base_url}{status_path}"
        session = self.get_session()
        response = session.get(url, timeout=self.build_timeout(), headers=headers, auth=auth)
        response.raise_for_status()
        return response

    def parse_payload(self, raw_payload: str, response: requests.Response | None) -> Any:
        if raw_payload.strip() == "":
            raise ValueError("Easee returned an empty payload.")
        try:
            return json.loads(raw_payload)
        except json.JSONDecodeError:
            return {"_raw_text": raw_payload}

    def normalize_payload(
        self, parsed_payload: Any, raw_payload: str
    ) -> tuple[list[MeasurementRecord], dict[str, Any]]:
        details = {
            "mapping_status": "adapter_skeleton",
            "todo": (
                "Easee local API mapping is firmware-specific and intentionally incomplete in v1. "
                "Verify the payload structure from your charger and extend this adapter."
            ),
        }
        measurements: list[MeasurementRecord] = []

        if isinstance(parsed_payload, dict):
            guessed_fields = {
                "wallbox_power_w": ["chargingPower", "power", "outputPower", "sessionPower"],
                "current_a": ["current", "outputCurrent", "dynamicChargerCurrent"],
                "voltage_v": ["voltage", "outputVoltage"],
                "session_energy_wh": ["sessionEnergy", "sessionEnergyWh"],
            }
            for metric_name, field_names in guessed_fields.items():
                for field_name in field_names:
                    if field_name in parsed_payload:
                        try:
                            measurements.append(
                                MeasurementRecord(
                                    metric_name=metric_name,
                                    metric_value=float(parsed_payload[field_name]),
                                    unit=self._default_unit(metric_name),
                                    raw_payload=raw_payload,
                                )
                            )
                            break
                        except (TypeError, ValueError) as exc:
                            raise ValueError(
                                f"Easee field '{field_name}' was present but not numeric."
                            ) from exc
            return measurements, details

        raise NotImplementedError(
            "Easee payload format is reachable but not mapped yet. Raw payload has been stored."
        )

    def collect(self):  # type: ignore[override]
        result = super().collect()
        if result.status == CollectorStatus.SUCCESS and not result.measurements:
            result.status = CollectorStatus.MAPPING_NOT_IMPLEMENTED
            result.success = False
            result.error_message = (
                "Easee responded, but no trusted field mapping exists yet. "
                "Raw payload has been stored for inspection."
            )
        return result

    def _default_unit(self, metric_name: str) -> str | None:
        if metric_name.endswith("_w"):
            return "W"
        if metric_name.endswith("_a"):
            return "A"
        if metric_name.endswith("_v"):
            return "V"
        if metric_name.endswith("_wh"):
            return "Wh"
        return None
