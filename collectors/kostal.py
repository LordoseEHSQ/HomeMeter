from __future__ import annotations

import socket
from datetime import datetime, timezone

from collectors.base import BaseCollector, CollectorStatus, MeasurementRecord
from services.kostal_mapping import (
    build_kostal_mapping_profile,
    discover_sunspec_models,
    format_discovery_payload,
    read_modbus_holding_registers,
)


class KostalCollector(BaseCollector):
    def collect(self):  # type: ignore[override]
        started = datetime.now(timezone.utc)
        host = str(self.config.get("host", "")).strip()
        port = int(self.config.get("port", 1502))
        protocol = str(self.config.get("protocol", "modbus_tcp")).lower()
        unit_id = int(self.config.get("unit_id", 71) or 71)
        mapping_profile = build_kostal_mapping_profile(self.config)
        transport_details = {
            "transport_state": "not_connected",
            "tcp_port_reachable": False,
            "protocol_response_state": "not_attempted",
            "decode_state": "not_implemented",
        }
        if not host:
            return self._result(
                started=started,
                status=CollectorStatus.OTHER_ERROR,
                success=False,
                error_message="KOSTAL collector missing host in config.",
                details={**transport_details, "mapping_profile": mapping_profile},
            )
        if protocol not in {"modbus_tcp", "sunspec_tcp"}:
            return self._result(
                started=started,
                status=CollectorStatus.UNSUPPORTED_RESPONSE,
                success=False,
                error_message=f"Unsupported KOSTAL protocol '{protocol}'.",
                details={**transport_details, "mapping_profile": mapping_profile},
            )

        try:
            with socket.create_connection((host, port), timeout=self.build_timeout()[0]):
                pass
            discovery = discover_sunspec_models(
                lambda start, qty: read_modbus_holding_registers(
                    host=host,
                    port=port,
                    unit_id=unit_id,
                    start_address=start,
                    quantity=qty,
                    timeout_seconds=self.build_timeout()[0],
                )
            )
            measurements = self._measurements_from_discovery(discovery)
            raw_payload = format_discovery_payload(discovery)
            verified_count = sum(1 for measurement in measurements if measurement.source_type == "verified")
            tentative_count = sum(1 for measurement in measurements if measurement.source_type == "tentative")
            unsupported_models = [
                model["model_id"] for model in discovery["models"] if model["decoder_support_state"] in {"unsupported", "discovered_only"}
            ]
            details = {
                "mapping_status": "partial" if tentative_count or unsupported_models else "verified",
                "protocol": protocol,
                "transport_state": "connected",
                "tcp_port_reachable": True,
                "protocol_response_state": "ok",
                "decode_state": "partial" if tentative_count or unsupported_models else "verified",
                "discovery_state": "ok",
                "verified_metric_count": verified_count,
                "tentative_metric_count": tentative_count,
                "discovered_models": [
                    {
                        "model_id": model["model_id"],
                        "model_length": model["model_length"],
                        "start_register": model["start_register"],
                        "end_register": model["end_register"],
                        "decoder_support_state": model["decoder_support_state"],
                    }
                    for model in discovery["models"]
                ],
                "unsupported_models": unsupported_models,
                "common_identity": next((model.get("identity", {}) for model in discovery["models"] if model["model_id"] == 1), {}),
                "mapping_profile": mapping_profile,
            }
            return self._result(
                started=started,
                status=CollectorStatus.SUCCESS if verified_count else CollectorStatus.MAPPING_NOT_IMPLEMENTED,
                success=verified_count > 0 or tentative_count > 0,
                raw_payload=raw_payload,
                measurements=measurements,
                details=details,
                error_message=(
                    None
                    if verified_count
                    else "SunSpec discovery worked, but only tentative or unsupported KOSTAL models are currently decoded."
                ),
            )
        except ValueError as exc:
            return self._result(
                started=started,
                status=CollectorStatus.PARSE_FAILURE,
                success=False,
                error_message=f"KOSTAL SunSpec parse/discovery failure: {exc}",
                details={
                    "protocol": protocol,
                    "transport_state": "connected",
                    "tcp_port_reachable": True,
                    "protocol_response_state": "parse_failed",
                    "decode_state": "failed",
                    "mapping_profile": mapping_profile,
                },
            )
        except TimeoutError as exc:
            return self._result(
                started=started,
                status=CollectorStatus.TIMEOUT,
                success=False,
                error_message=f"KOSTAL TCP timeout on {host}:{port}: {exc}",
                details={**transport_details, "protocol": protocol, "mapping_profile": mapping_profile},
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
                details={**transport_details, "protocol": protocol, "mapping_profile": mapping_profile},
            )

    def _measurements_from_discovery(self, discovery: dict[str, object]) -> list[MeasurementRecord]:
        measurements: list[MeasurementRecord] = []
        for model in discovery.get("models", []):
            if not isinstance(model, dict):
                continue
            for measurement in model.get("measurements", []):
                if not isinstance(measurement, dict):
                    continue
                measurements.append(
                    MeasurementRecord(
                        metric_name=str(measurement["metric_name"]),
                        metric_value=float(measurement["metric_value"]),
                        unit=measurement.get("unit"),
                        source_type=str(measurement.get("source_type", "tentative")),
                        raw_payload=None,
                    )
                )
        return measurements
