from __future__ import annotations

import json
import logging
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from collectors.base import BaseCollector, MeasurementRecord

LOGGER = logging.getLogger(__name__)


class CfosCollector(BaseCollector):
    source_type = "cfos_http"

    def perform_request(self) -> requests.Response:
        base_url = str(self.config.get("base_url", "")).rstrip("/")
        status_path = str(self.config.get("status_path", "/"))
        if not base_url:
            raise ValueError("cFos collector missing base_url in config.")

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
        content_type = response.headers.get("Content-Type", "") if response is not None else ""
        if "json" in content_type.lower() or raw_payload.strip().startswith(("{", "[")):
            try:
                return json.loads(raw_payload)
            except json.JSONDecodeError as exc:
                raise ValueError(f"cFos JSON parse failure: {exc}") from exc
        LOGGER.warning("cFos returned non-JSON payload; storing raw payload only.")
        return {"_raw_text": raw_payload, "_todo": "Unknown cFos response format"}

    def normalize_payload(
        self, parsed_payload: Any, raw_payload: str
    ) -> tuple[list[MeasurementRecord], dict[str, Any]]:
        measurements: list[MeasurementRecord] = []
        details: dict[str, Any] = {"mapping_status": "partial"}

        if not isinstance(parsed_payload, dict):
            raise ValueError("cFos response could not be normalized; expected JSON object.")

        known_candidates = {
            "grid_power_w": ["grid_power", "gridPower", "power_grid", "powerGrid"],
            "house_power_w": ["house_power", "housePower", "home_power", "load_power"],
            "wallbox_power_w": ["charging_power", "charger_power", "wallbox_power", "power"],
            "pv_power_w": ["pv_power", "pvPower", "solar_power"],
            "current_a": ["current", "amps", "current_a"],
            "voltage_v": ["voltage", "voltage_v"],
            "energy_wh": ["energy", "energy_wh", "charged_energy"],
        }

        for metric_name, field_names in known_candidates.items():
            value = self._extract_number(parsed_payload, field_names)
            if value is not None:
                measurements.append(
                    MeasurementRecord(
                        metric_name=metric_name,
                        metric_value=value,
                        unit=self._default_unit(metric_name),
                        raw_payload=raw_payload,
                    )
                )

        numeric_pairs = self._flatten_numeric_values(parsed_payload)
        details["numeric_field_count"] = len(numeric_pairs)
        details["todo"] = (
            "Field mapping is best-effort only. Verify cFos API fields against your device "
            "and extend collectors/cfos.py for trusted semantics."
        )
        if not measurements and numeric_pairs:
            for key, value in list(numeric_pairs.items())[:10]:
                measurements.append(
                    MeasurementRecord(
                        metric_name=f"raw::{key}",
                        metric_value=value,
                        unit=None,
                        source_type="raw_numeric",
                        raw_payload=raw_payload,
                    )
                )
        return measurements, details

    def _extract_number(self, payload: dict[str, Any], candidates: list[str]) -> float | None:
        for candidate in candidates:
            if candidate in payload:
                try:
                    return float(payload[candidate])
                except (TypeError, ValueError):
                    LOGGER.warning("cFos field %s exists but is not numeric.", candidate)
        return None

    def _flatten_numeric_values(self, value: Any, prefix: str = "") -> dict[str, float]:
        flattened: dict[str, float] = {}
        if isinstance(value, dict):
            for key, item in value.items():
                next_prefix = f"{prefix}.{key}" if prefix else str(key)
                flattened.update(self._flatten_numeric_values(item, next_prefix))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                next_prefix = f"{prefix}[{index}]"
                flattened.update(self._flatten_numeric_values(item, next_prefix))
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            flattened[prefix or "value"] = float(value)
        return flattened

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
