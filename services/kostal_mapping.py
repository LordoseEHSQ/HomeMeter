from __future__ import annotations

import struct
from typing import Any


SUPPORTED_BYTE_ORDERS = {"ABCD", "BADC", "CDAB", "DCBA"}
SUPPORTED_DATA_TYPES = {"u16", "i16", "u32", "i32", "float32"}


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
        "supported_decoders": sorted(SUPPORTED_DATA_TYPES),
        "candidate_measurements": [
            {
                "metric_name": "ac_active_power_w",
                "mapping_state": "prepared",
                "confidence": "tentative",
                "note": "Preferred target for later Modbus/SunSpec verification.",
            },
            {
                "metric_name": "pv_power_w",
                "mapping_state": "prepared",
                "confidence": "tentative",
                "note": "Expected to come from verified inverter/SunSpec model discovery later.",
            },
            {
                "metric_name": "grid_power_w",
                "mapping_state": "prepared",
                "confidence": "tentative",
                "note": "Do not trust sign semantics until live register validation is completed.",
            },
            {
                "metric_name": "battery_power_w",
                "mapping_state": "open",
                "confidence": "unknown",
                "note": "Battery-related mapping is not verified for this installation yet.",
            },
        ],
        "implementation_note": (
            "Connectivity and byte-order handling are modeled. Live register addresses and sign conventions "
            "remain intentionally unverified until real KOSTAL responses are captured."
        ),
    }


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


def _to_signed_16(value: int) -> int:
    return value - 0x10000 if value & 0x8000 else value
