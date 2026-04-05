from services.kostal_mapping import (
    apply_scale,
    build_kostal_mapping_profile,
    decode_common_model,
    decode_model_103,
    decode_register_value,
    discover_sunspec_models,
    format_discovery_payload,
)


def test_decode_register_value_supports_cdab_float32():
    value = decode_register_value([0x0000, 0x4120], "float32", "CDAB")
    assert value == 10.0


def test_apply_scale_handles_negative_scale():
    assert apply_scale(2377, -1) == 237.7


def test_decode_common_model_extracts_identity_strings():
    manufacturer = [0x4B4F, 0x5354, 0x414C] + [0] * 13
    model = [0x504C, 0x454E, 0x5449, 0x434F, 0x5245] + [0] * 11
    registers = manufacturer + model + [0] * (66 - len(manufacturer) - len(model))
    registers[64] = 71
    identity = decode_common_model(registers)
    assert identity["manufacturer"] == "KOSTAL"
    assert "PLENTICORE" in identity["model"]
    assert identity["device_address"] == 71


def test_discover_sunspec_models_walks_models_and_stops_at_end_marker():
    register_map = {
        40000: [0x5375, 0x6E53],
        40002: [1, 66],
        40004: [0] * 66,
        40070: [103, 50],
        40072: [
            328, 110, 111, 107, 0xFFFE,
            0xFFFF, 0xFFFF, 2377, 2369, 2376, 0xFFFF, 0xFFFF, 712, 0, 4996, 0xFFFE,
            774, 0, 10, 0, 995, 0xFFFE, 0, 100, 0, 125, 0xFFFE, 540, 0xFFFE, 675, 0, 250, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        ],
        40122: [0xFFFF, 0],
    }

    def fake_read(start: int, qty: int) -> list[int]:
        return register_map[start]

    discovery = discover_sunspec_models(fake_read)
    assert discovery["marker_found"] is True
    assert [model["model_id"] for model in discovery["models"]] == [1, 103]
    assert discovery["end_marker_found"] is True
    assert discovery["models"][1]["decoder_support_state"] == "partial"


def test_format_discovery_payload_includes_discovered_model_metadata():
    discovery = {
        "marker_found": True,
        "models": [{"model_id": 1, "start_register": 40002, "end_register": 40069, "decoder_support_state": "verified_identity"}],
    }
    payload = format_discovery_payload(discovery)

    assert '"model_id": 1' in payload
    assert '"decoder_support_state": "verified_identity"' in payload


def test_decode_model_103_produces_verified_and_tentative_metrics():
    registers = [
        328, 110, 111, 107, 0xFFFE,
        0xFFFF, 0xFFFF, 2377, 2369, 2376, 0xFFFF, 0xFFFF, 712, 0, 4996, 0xFFFE,
        774, 0, 10, 0, 995, 0xFFFE, 0, 100, 0, 125, 0xFFFE, 540, 0xFFFE, 675, 0, 250, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    ]
    decoded = decode_model_103(registers)
    names = {field["name"] for field in decoded["decoded_fields"]}
    assert "inverter_ac_power_w" in names
    assert "inverter_frequency_hz" in names
    assert any(measurement["source_type"] == "verified" for measurement in decoded["measurements"])
    assert any(measurement["source_type"] == "tentative" for measurement in decoded["measurements"])
    assert any(field["classification"] == "discovered_but_unsupported" for field in decoded["decoded_fields"])


def test_build_kostal_mapping_profile_exposes_supported_models(sample_config_dict):
    profile = build_kostal_mapping_profile(sample_config_dict["devices"]["kostal"])
    assert profile["unit_id"] == 71
    assert profile["modbus_byte_order"] == "CDAB"
    assert profile["supported_models"]["103"] == "verified_and_tentative_measurements"
