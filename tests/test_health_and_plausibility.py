from analysis.plausibility import PlausibilityEngine
from collectors.base import CollectorStatus
from services.config_validation import ConfigValidator
from services.database_stats import DatabaseInspector
from services.device_specs import build_device_specs
from services.diagnostics import build_device_operations_view
from services.health import SystemHealthService

from tests.conftest import insert_poll_result


def test_plausibility_detects_missing_values(store):
    insert_poll_result(store, device_name="cfos", metric_name="grid_power_w", metric_value=100)
    alerts = PlausibilityEngine(store).run(["cfos"], "2026-04-05T12:00:00+00:00")
    assert any(alert.rule_name == "missing_values" for alert in alerts)


def test_health_service_marks_degraded_when_device_times_out(store, sample_config_dict):
    insert_poll_result(
        store,
        device_name="cfos",
        status=CollectorStatus.TIMEOUT,
        success=False,
        metric_name=None,
    )
    validation = ConfigValidator().validate(sample_config_dict)
    specs = build_device_specs(sample_config_dict)
    operations = [
        build_device_operations_view("cfos", sample_config_dict["devices"]["cfos"], store, validation, specs["cfos"]),
        build_device_operations_view("easee", sample_config_dict["devices"]["easee"], store, validation, specs["easee"]),
        build_device_operations_view("kostal", sample_config_dict["devices"]["kostal"], store, validation, specs["kostal"]),
    ]
    report = DatabaseInspector(store).build_report()
    summary = SystemHealthService(store).build_summary(operations, validation, report)
    assert summary["status"] in {"warning", "degraded"}
    assert summary["recent_failures"] >= 1
