from services.kostal_mapping import build_kostal_mapping_profile, decode_register_value


def test_decode_register_value_supports_cdab_float32():
    value = decode_register_value([0x0000, 0x4120], "float32", "CDAB")
    assert value == 10.0


def test_decode_register_value_supports_signed_16bit():
    value = decode_register_value([0xFF9C], "i16", "ABCD")
    assert value == -100


def test_build_kostal_mapping_profile_exposes_tentative_candidates(sample_config_dict):
    profile = build_kostal_mapping_profile(sample_config_dict["devices"]["kostal"])
    assert profile["unit_id"] == 71
    assert profile["modbus_byte_order"] == "CDAB"
    assert any(item["metric_name"] == "grid_power_w" for item in profile["candidate_measurements"])
