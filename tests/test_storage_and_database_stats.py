from services.database_stats import DatabaseInspector

from tests.conftest import insert_poll_result


def test_database_initialization_creates_empty_tables(store):
    report = DatabaseInspector(store).build_report()
    assert report["file_exists"] is True
    assert report["tables"]["measurements"]["row_count"] == 0
    assert report["tables"]["poll_events"]["row_count"] == 0
    assert report["tables"]["alerts"]["row_count"] == 0
    assert report["tables"]["semantic_metrics"]["row_count"] == 0
    assert report["tables"]["minute_rollups"]["row_count"] == 0
    assert report["tables"]["kpi_summaries"]["row_count"] == 0


def test_database_report_reflects_stored_rows(store):
    insert_poll_result(store, device_name="cfos")
    store.save_alert(
        timestamp_utc="2026-04-05T12:00:01+00:00",
        device_name="cfos",
        severity="medium",
        rule_name="test_rule",
        message="Test alert",
        context={"ok": False},
    )
    report = DatabaseInspector(store).build_report()
    assert report["tables"]["measurements"]["row_count"] == 1
    assert report["tables"]["poll_events"]["row_count"] == 1
    assert report["tables"]["alerts"]["row_count"] == 1
    assert report["storage_activity"]["status"] == "warning"


def test_database_report_includes_per_device_recording_summary(store):
    insert_poll_result(store, device_name="cfos", metric_name="raw::foo", metric_value=5.0)
    report = DatabaseInspector(store).build_report()
    assert report["per_device_recording"]["cfos"]["poll_event_count"] == 1
    assert report["per_device_recording"]["cfos"]["measurement_count"] == 1
    assert report["per_device_recording"]["cfos"]["unmapped_numeric_measurement_count"] == 0


def test_database_report_tracks_verified_and_candidate_recording_counts(store):
    insert_poll_result(
        store,
        device_name="kostal",
        metric_name="inverter_ac_power_w",
        metric_value=4200.0,
        source_type="verified",
    )
    insert_poll_result(
        store,
        device_name="kostal",
        metric_name="candidate::dc_power",
        metric_value=3000.0,
        source_type="likely_useful_candidate",
    )
    insert_poll_result(
        store,
        device_name="kostal",
        metric_name="raw::mystery",
        metric_value=7.0,
        source_type="unmapped_numeric",
    )
    report = DatabaseInspector(store).build_report()

    assert report["per_device_recording"]["kostal"]["measurement_count"] == 3
    assert report["per_device_recording"]["kostal"]["verified_measurement_count"] == 1
    assert report["per_device_recording"]["kostal"]["likely_candidate_measurement_count"] == 1
    assert report["per_device_recording"]["kostal"]["unmapped_numeric_measurement_count"] == 1
