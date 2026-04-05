from datetime import datetime, timedelta, timezone

from services.database_stats import DatabaseInspector

from collectors.base import CollectorResult, CollectorStatus, MeasurementRecord
from storage.sqlite_store import KPIRecord, SemanticMetricRecord
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
    assert report["tables"]["cleanup_runs"]["row_count"] == 0


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


def test_store_can_persist_poll_event_without_measurement_write(store):
    result = CollectorResult(
        device_name="cfos",
        source_type="test",
        status=CollectorStatus.SUCCESS,
        success=True,
        timestamp_utc="2026-04-05T12:10:00Z",
        duration_ms=30,
        measurements=[MeasurementRecord(metric_name="grid_power_w", metric_value=500.0, unit="W", source_type="confirmed_useful")],
        raw_payload='{"payload": true}',
        details={"write_mode": "poll_only"},
    )
    store.save_collector_result(result, include_measurements=False, include_raw_payload=False)
    report = DatabaseInspector(store).build_report()
    assert report["tables"]["poll_events"]["row_count"] == 1
    assert report["tables"]["measurements"]["row_count"] == 0
    assert report["raw_payload_rows"] == 0


def test_store_cleanup_old_data_removes_old_raw_and_rollups(store):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    old_ts = (now - timedelta(days=40)).isoformat()
    recent_ts = (now - timedelta(days=1)).isoformat()
    old_bucket = (now - timedelta(days=40)).replace(second=0).isoformat()
    recent_bucket = (now - timedelta(days=1)).replace(second=0).isoformat()

    insert_poll_result(store, device_name="cfos", timestamp_utc=old_ts)
    insert_poll_result(store, device_name="cfos", timestamp_utc=recent_ts)
    store.save_semantic_metrics(
        [
            SemanticMetricRecord(
                timestamp_utc=old_ts,
                device_name="system",
                metric_name="house_consumption_w",
                metric_value=1000.0,
                unit="W",
                classification="derived",
                formula_version="test",
                source_metric_names=["grid_import_w"],
                source_coverage=1.0,
                confidence_state="high",
                details={},
            ),
            SemanticMetricRecord(
                timestamp_utc=recent_ts,
                device_name="system",
                metric_name="house_consumption_w",
                metric_value=1200.0,
                unit="W",
                classification="derived",
                formula_version="test",
                source_metric_names=["grid_import_w"],
                source_coverage=1.0,
                confidence_state="high",
                details={},
            ),
        ]
    )
    store.refresh_minute_rollups(old_bucket, ["house_consumption_w"])
    store.refresh_minute_rollups(recent_bucket, ["house_consumption_w"])
    store.upsert_kpi_summaries(
        [
            KPIRecord(
                window_key="24h",
                window_start_utc=old_ts,
                window_end_utc=old_ts,
                metric_name="house_consumption_kwh",
                metric_value=12.0,
                unit="kWh",
                classification="derived",
                formula_version="test",
                source_coverage=1.0,
                confidence_state="high",
                updated_at_utc=old_ts,
                details={},
            ),
            KPIRecord(
                window_key="7d",
                window_start_utc=recent_ts,
                window_end_utc=recent_ts,
                metric_name="house_consumption_kwh",
                metric_value=5.0,
                unit="kWh",
                classification="derived",
                formula_version="test",
                source_coverage=1.0,
                confidence_state="high",
                updated_at_utc=recent_ts,
                details={},
            ),
        ]
    )
    store.save_alert(
        timestamp_utc=old_ts,
        device_name="cfos",
        severity="medium",
        rule_name="old_alert",
        message="old",
        context={},
    )
    store.save_alert(
        timestamp_utc=recent_ts,
        device_name="cfos",
        severity="medium",
        rule_name="recent_alert",
        message="recent",
        context={},
    )

    cleanup_result = store.cleanup_old_data(retention_days_raw=30, retention_days_rollup=30)
    report = DatabaseInspector(store).build_report()
    cleanup_runs = store.get_recent_cleanup_runs(limit=20)

    assert cleanup_result["measurements_deleted"] >= 1
    assert cleanup_result["poll_events_deleted"] >= 1
    assert cleanup_result["alerts_deleted"] >= 1
    assert cleanup_result["semantic_metrics_deleted"] >= 1
    assert cleanup_result["minute_rollups_deleted"] >= 1
    assert cleanup_result["kpi_summaries_deleted"] >= 1
    assert report["tables"]["measurements"]["row_count"] == 1
    assert report["tables"]["poll_events"]["row_count"] == 1
    assert report["tables"]["alerts"]["row_count"] == 1
    assert report["tables"]["semantic_metrics"]["row_count"] == 1
    assert report["tables"]["minute_rollups"]["row_count"] == 1
    assert report["tables"]["kpi_summaries"]["row_count"] == 1
    assert report["tables"]["cleanup_runs"]["row_count"] == 6
    assert any(run["cleanup_scope"] == "measurements" for run in cleanup_runs)
    assert any(run["cleanup_scope"] == "kpi_summaries" for run in cleanup_runs)
