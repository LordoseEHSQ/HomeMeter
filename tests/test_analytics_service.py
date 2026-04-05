from datetime import datetime, timezone

from collectors.base import CollectorResult, CollectorStatus, MeasurementRecord
from services.analytics import AnalyticsEngine, build_svg_points, resolve_window
from services.time_utils import TimeSettings


def _save_metrics(store, timestamp_utc: str, metrics: dict[str, float]) -> None:
    result = CollectorResult(
        device_name="cfos",
        source_type="test",
        status=CollectorStatus.SUCCESS,
        success=True,
        timestamp_utc=timestamp_utc,
        duration_ms=25,
        measurements=[
            MeasurementRecord(metric_name=name, metric_value=value, unit="W", source_type="confirmed_useful")
            for name, value in metrics.items()
        ],
        raw_payload='{"mock": true}',
        http_status=200,
        error_message=None,
        details={},
    )
    store.save_collector_result(result)


def test_analytics_engine_persists_semantic_metrics_rollups_and_kpis(store):
    engine = AnalyticsEngine(store, settings={"default_window": "24h"})
    timestamps = [
        "2026-04-05T10:00:05Z",
        "2026-04-05T10:01:05Z",
        "2026-04-05T10:02:05Z",
    ]
    for timestamp in timestamps:
        _save_metrics(
            store,
            timestamp,
            {
                "grid_power_w": 1000.0,
                "house_power_w": 3000.0,
                "wallbox_power_w": 1000.0,
                "pv_power_w": 2500.0,
            },
        )
        engine.process_cycle(timestamp)

    status = store.get_analytics_status()
    assert status["semantic_metric_count"] > 0
    assert status["rollup_count"] > 0
    assert status["kpi_summary_count"] > 0

    rows = store.get_kpi_summaries("24h")
    by_name = {row["metric_name"]: row for row in rows}
    assert round(by_name["pv_generation_kwh"]["metric_value"], 3) == 0.125
    assert round(by_name["grid_import_kwh"]["metric_value"], 3) == 0.05
    assert round(by_name["house_consumption_kwh"]["metric_value"], 3) == 0.15
    assert round(by_name["wallbox_energy_kwh"]["metric_value"], 3) == 0.05
    assert round(by_name["self_consumption_kwh"]["metric_value"], 3) == 0.125
    assert round(by_name["self_sufficiency_ratio"]["metric_value"], 1) == 66.7
    assert by_name["self_sufficiency_ratio"]["classification"] == "estimated"


def test_analytics_engine_derives_pv_from_inverter_when_direct_pv_missing(store):
    engine = AnalyticsEngine(store)
    result = CollectorResult(
        device_name="kostal",
        source_type="test",
        status=CollectorStatus.SUCCESS,
        success=True,
        timestamp_utc="2026-04-05T11:00:05Z",
        duration_ms=30,
        measurements=[
            MeasurementRecord(metric_name="grid_power_w", metric_value=-500.0, unit="W", source_type="confirmed_useful"),
            MeasurementRecord(metric_name="inverter_ac_power_w", metric_value=1800.0, unit="W", source_type="verified"),
        ],
        raw_payload='{"mock": true}',
        http_status=200,
        error_message=None,
        details={},
    )
    store.save_collector_result(result)

    engine.process_cycle("2026-04-05T11:00:05Z")
    semantic_rows = store.get_latest_rows("semantic_metrics", limit=20)
    pv_rows = [row for row in semantic_rows if row["metric_name"] == "pv_power_w"]
    assert pv_rows
    assert pv_rows[0]["classification"] == "estimated"


def test_resolve_window_and_svg_points_behave_consistently():
    now = datetime(2026, 4, 5, 12, 0, 0, tzinfo=timezone.utc)
    start, end = resolve_window("24h", now)
    assert end > start
    assert int((end - start).total_seconds()) == 24 * 3600
    points = build_svg_points([1.0, 2.0, 3.0], width=100, height=50)
    assert "0.0," in points or "0," in points


def test_build_dashboard_returns_localized_chart_metadata(store):
    engine = AnalyticsEngine(store, settings={"default_window": "24h", "chart_refresh_seconds": 30})
    for index in range(3):
        timestamp = f"2026-04-05T10:0{index}:05Z"
        _save_metrics(
            store,
            timestamp,
            {
                "grid_power_w": 400.0,
                "house_power_w": 1600.0,
                "wallbox_power_w": 1100.0,
                "pv_power_w": 2200.0,
            },
        )
        engine.process_cycle(timestamp)

    dashboard = engine.build_dashboard(
        "24h",
        language="de",
        time_settings=TimeSettings(display_timezone="Europe/Berlin", display_format="%d.%m.%Y %H:%M:%S"),
    )

    assert dashboard["window_label"] == "Letzte 24 Stunden"
    assert dashboard["refresh_seconds"] == 30
    first_chart = dashboard["chart_series"][0]
    assert first_chart["label"] == "PV-Leistung"
    assert first_chart["empty"] is False
    assert first_chart["display_unit"] in {"W", "kW"}
    assert len(first_chart["x_ticks"]) >= 2
    assert len(first_chart["y_ticks"]) >= 2
