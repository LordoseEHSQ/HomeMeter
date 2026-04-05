from services.device_specs import build_device_specs
from services.gap_detection import build_integration_gaps
from services.config_validation import ConfigValidator
from services.diagnostics import build_device_operations_view

from tests.conftest import insert_poll_result


def test_device_specs_parse_cfos_and_kostal_details(sample_config_dict):
    specs = build_device_specs(sample_config_dict)
    assert specs["cfos"]["device_type"] == "cfos_wallbox_booster"
    assert specs["cfos"]["protocols"]["http"]["enabled"] is True
    assert specs["cfos"]["auth_model"]["password_configured"] is True
    assert specs["kostal"]["unit_id"] == 71
    assert specs["kostal"]["modbus_byte_order"] == "CDAB"
    assert specs["kostal"]["auth_model"]["role"] == "plant_owner"


def test_gap_detection_flags_reachable_without_metrics(store, sample_config_dict):
    insert_poll_result(
        store,
        device_name="cfos",
        metric_name=None,
    )
    validation = ConfigValidator().validate(sample_config_dict)
    specs = build_device_specs(sample_config_dict)
    operations = [
        build_device_operations_view("cfos", sample_config_dict["devices"]["cfos"], store, validation, specs["cfos"]),
        build_device_operations_view("easee", sample_config_dict["devices"]["easee"], store, validation, specs["easee"]),
        build_device_operations_view("kostal", sample_config_dict["devices"]["kostal"], store, validation, specs["kostal"]),
    ]
    gaps = build_integration_gaps(store, operations, specs)
    assert any("no normalized metrics" in gap["message"].lower() for gap in gaps)
