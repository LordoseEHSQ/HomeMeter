from __future__ import annotations

import json
import socket
import struct
from typing import Any, Callable


SUPPORTED_BYTE_ORDERS = {"ABCD", "BADC", "CDAB", "DCBA"}
SUPPORTED_DATA_TYPES = {"u16", "i16", "u32", "i32", "float32"}
SUNSPEC_BASE_REGISTER = 40000
SUNSPEC_END_MARKER = 0xFFFF


def decode_register_value(registers: list[int], data_type: str, byte_order: str = "ABCD") -> float | int:
    normalized_type = data_type.lower()
    normalized_order = byte_order.upper()
    if normalized_type not in SUPPORTED_DATA_TYPES:
        raise ValueError(f"Unsupported KOSTAL data type '{data_type}'.")
    if normalized_order not in SUPPORTED_BYTE_ORDERS:
        raise ValueError(f"Unsupported byte order '{byte_order}'.")
    if normalized_type in {"u16", "i16"}:
        if len(registers) < 1:
            raise ValueError("At least one register is required for 16-bit decoding.")
        raw = registers[0] & 0xFFFF
        return raw if normalized_type == "u16" else _to_signed_16(raw)
    if len(registers) < 2:
        raise ValueError("At least two registers are required for 32-bit decoding.")

    payload = _registers_to_bytes(registers[:2], normalized_order)
    if normalized_type == "u32":
        return struct.unpack(">I", payload)[0]
    if normalized_type == "i32":
        return struct.unpack(">i", payload)[0]
    return round(struct.unpack(">f", payload)[0], 6)


def build_kostal_mapping_profile(config: dict[str, Any]) -> dict[str, Any]:
    protocol = str(config.get("protocol", "modbus_tcp")).lower()
    return {
        "protocol": protocol,
        "unit_id": int(config.get("unit_id", 71) or 71),
        "modbus_byte_order": str(config.get("modbus_byte_order", "CDAB")).upper(),
        "sunspec_byte_order": str(config.get("sunspec_byte_order", "ABCD")).upper(),
        "mapping_state": "partial",
        "supported_decoders": sorted(SUPPORTED_DATA_TYPES),
        "supported_models": {
            "1": "verified_identity",
            "103": "verified_and_tentative_measurements",
            "113": "discovered_only",
        },
        "candidate_measurements": [
            {
                "metric_name": "inverter_ac_power_w",
                "mapping_state": "verified",
                "confidence": "verified",
                "note": "Decoded from SunSpec inverter model 103 active power.",
            },
            {
                "metric_name": "inverter_frequency_hz",
                "mapping_state": "verified",
                "confidence": "verified",
                "note": "Decoded from SunSpec inverter model 103 frequency field.",
            },
            {
                "metric_name": "pv_power_w",
                "mapping_state": "tentative",
                "confidence": "tentative",
                "note": "Likely related to inverter production, but system-level semantics are still being validated.",
            },
            {
                "metric_name": "battery_power_w",
                "mapping_state": "unknown",
                "confidence": "unknown",
                "note": "Battery-related mapping is not verified for this installation yet.",
            },
        ],
        "implementation_note": (
            "SunSpec discovery is implemented. Common model identity and inverter model 103 decoding are "
            "partially grounded; further models remain visible even when unsupported."
        ),
    }


def read_modbus_holding_registers(
    host: str,
    port: int,
    unit_id: int,
    start_address: int,
    quantity: int,
    timeout_seconds: float,
) -> list[int]:
    transaction_id = (start_address % 65000) + 1
    pdu = struct.pack(">BHH", 3, start_address, quantity)
    mbap = struct.pack(">HHHB", transaction_id, 0, len(pdu) + 1, unit_id)
    request = mbap + pdu
    with socket.create_connection((host, port), timeout=timeout_seconds) as sock:
        sock.settimeout(timeout_seconds)
        sock.sendall(request)
        response = sock.recv(max(256, quantity * 2 + 16))
    if len(response) < 9:
        raise ValueError(f"Short Modbus response while reading {start_address}/{quantity}.")
    function_code = response[7]
    if function_code == 0x83:
        exception_code = response[8] if len(response) > 8 else None
        raise ValueError(f"Modbus exception {exception_code} at address {start_address}.")
    if function_code != 3:
        raise ValueError(f"Unexpected Modbus function code {function_code} at address {start_address}.")
    byte_count = response[8]
    data = response[9 : 9 + byte_count]
    return [int.from_bytes(data[i : i + 2], "big") for i in range(0, len(data), 2)]


def discover_sunspec_models(
    read_registers: Callable[[int, int], list[int]],
    start_register: int = SUNSPEC_BASE_REGISTER,
    max_models: int = 32,
) -> dict[str, Any]:
    marker_registers = read_registers(start_register, 2)
    marker = registers_to_string(marker_registers)
    if marker != "SunS":
        raise ValueError(f"SunSpec marker not found at register {start_register}: {marker!r}")

    cursor = start_register + 2
    models: list[dict[str, Any]] = []
    end_marker_found = False
    for _ in range(max_models):
        header = read_registers(cursor, 2)
        model_id = header[0]
        model_length = header[1]
        if model_id == SUNSPEC_END_MARKER:
            end_marker_found = True
            break
        registers = read_registers(cursor + 2, model_length)
        decoded = decode_sunspec_model(model_id, registers, start_register=cursor)
        models.append(
            {
                "model_id": model_id,
                "model_length": model_length,
                "start_register": cursor,
                "end_register": cursor + 1 + model_length,
                "decoder_support_state": decoded["decoder_support_state"],
                "decoded_fields": decoded["decoded_fields"],
                "measurements": decoded["measurements"],
                "identity": decoded.get("identity", {}),
                "raw_preview": registers[: min(len(registers), 16)],
            }
        )
        cursor += 2 + model_length
    return {
        "marker_found": True,
        "marker_register": start_register,
        "models": models,
        "end_marker_found": end_marker_found,
        "next_register": cursor,
    }


def decode_sunspec_model(model_id: int, registers: list[int], start_register: int) -> dict[str, Any]:
    if model_id == 1:
        identity = decode_common_model(registers)
        return {
            "decoder_support_state": "verified_identity",
            "decoded_fields": [{"name": key, "value": value, "classification": "verified"} for key, value in identity.items()],
            "measurements": [],
            "identity": identity,
        }
    if model_id == 103:
        return decode_model_103(registers)
    if model_id == 113:
        return {
            "decoder_support_state": "discovered_only",
            "decoded_fields": [{"name": f"reg_{start_register + 2 + idx}", "value": reg, "classification": "discovered"} for idx, reg in enumerate(registers[:16])],
            "measurements": [],
        }
    return {
        "decoder_support_state": "unsupported",
        "decoded_fields": [{"name": "raw_register_count", "value": len(registers), "classification": "discovered"}],
        "measurements": [],
    }


def decode_common_model(registers: list[int]) -> dict[str, str | int]:
    return {
        "manufacturer": registers_to_string(registers[0:16]),
        "model": registers_to_string(registers[16:32]),
        "options": registers_to_string(registers[32:40]),
        "version": registers_to_string(registers[40:48]),
        "serial_number": registers_to_string(registers[48:64]),
        "device_address": registers[64] if len(registers) > 64 else 0,
    }


def decode_model_103(registers: list[int]) -> dict[str, Any]:
    if len(registers) < 50:
        raise ValueError("SunSpec model 103 payload is shorter than expected.")

    fields: list[dict[str, Any]] = []
    measurements: list[dict[str, Any]] = []
    a_sf = _signed16(registers[4])
    v_sf = _signed16(registers[11])
    w_sf = _signed16(registers[13])
    hz_sf = _signed16(registers[15])
    va_sf = _signed16(registers[17])
    var_sf = _signed16(registers[19])
    pf_sf = _signed16(registers[21])
    wh_sf = _signed16(registers[24])
    dc_a_sf = _signed16(registers[26])
    dc_v_sf = _signed16(registers[28])
    dc_w_sf = _signed16(registers[30])
    tmp_sf = _signed16(registers[35])

    metric_defs = [
        ("inverter_ac_current_a", registers[0], a_sf, "A", "verified"),
        ("inverter_ac_current_phase_a_a", registers[1], a_sf, "A", "verified"),
        ("inverter_ac_current_phase_b_a", registers[2], a_sf, "A", "verified"),
        ("inverter_ac_current_phase_c_a", registers[3], a_sf, "A", "verified"),
        ("inverter_voltage_phase_a_v", registers[7], v_sf, "V", "verified"),
        ("inverter_voltage_phase_b_v", registers[8], v_sf, "V", "verified"),
        ("inverter_voltage_phase_c_v", registers[9], v_sf, "V", "verified"),
        ("inverter_ac_power_w", registers[12], w_sf, "W", "verified"),
        ("inverter_frequency_hz", registers[14], hz_sf, "Hz", "verified"),
        ("inverter_apparent_power_va", registers[16], va_sf, "VA", "verified"),
        ("inverter_reactive_power_var", registers[18], var_sf, "VAr", "verified"),
        ("inverter_power_factor_pct", registers[20], pf_sf, "%", "tentative"),
        ("inverter_energy_wh", decode_register_value(registers[22:24], "u32"), wh_sf, "Wh", "verified"),
        ("inverter_dc_current_a", registers[25], dc_a_sf, "A", "tentative"),
        ("inverter_dc_voltage_v", registers[27], dc_v_sf, "V", "tentative"),
        ("inverter_dc_power_w", registers[29], dc_w_sf, "W", "tentative"),
        ("inverter_temperature_c", registers[32], tmp_sf, "°C", "tentative"),
    ]

    for metric_name, raw_value, scale_factor, unit, classification in metric_defs:
        value = apply_scale(raw_value, scale_factor)
        if value is None:
            continue
        fields.append(
            {
                "name": metric_name,
                "raw_value": raw_value,
                "value": value,
                "unit": unit,
                "classification": classification,
            }
        )
        measurements.append(
            {
                "metric_name": metric_name,
                "metric_value": value,
                "unit": unit,
                "source_type": classification,
            }
        )

    unsupported_fields = [
        {"name": "model_103_status", "raw_value": registers[36], "classification": "discovered_but_unsupported"},
        {"name": "model_103_status_vendor", "raw_value": registers[37], "classification": "discovered_but_unsupported"},
    ]
    fields.extend(unsupported_fields)
    return {
        "decoder_support_state": "partial",
        "decoded_fields": fields,
        "measurements": measurements,
    }


def apply_scale(raw_value: int | float | None, scale_factor: int | None) -> float | None:
    if raw_value is None or scale_factor is None:
        return None
    if raw_value in {0xFFFF, 0xFFFE, 0x8000}:
        return None
    value = float(raw_value)
    return round(value * (10 ** int(scale_factor)), 6)


def registers_to_string(registers: list[int]) -> str:
    payload = b"".join(register.to_bytes(2, "big") for register in registers)
    return payload.replace(b"\x00", b"").decode("ascii", errors="ignore").strip()


def format_discovery_payload(discovery: dict[str, Any]) -> str:
    return json.dumps(discovery, indent=2, sort_keys=False)


def _registers_to_bytes(registers: list[int], byte_order: str) -> bytes:
    first = registers[0] & 0xFFFF
    second = registers[1] & 0xFFFF
    a, b = divmod(first, 0x100)
    c, d = divmod(second, 0x100)
    ordered = {
        "ABCD": bytes([a, b, c, d]),
        "BADC": bytes([b, a, d, c]),
        "CDAB": bytes([c, d, a, b]),
        "DCBA": bytes([d, c, b, a]),
    }
    return ordered[byte_order]


def _signed16(value: int) -> int:
    return value - 0x10000 if value & 0x8000 else value


def _to_signed_16(value: int) -> int:
    return _signed16(value)
