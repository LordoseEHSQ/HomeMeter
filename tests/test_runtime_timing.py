from app import build_runtime_hygiene
from services.runtime_timing import RuntimeTimingCoordinator, RuntimeTimingSettings, load_runtime_timing_settings


def test_load_runtime_timing_settings_reads_separate_intervals(sample_config_dict):
    settings = load_runtime_timing_settings(sample_config_dict)
    assert settings.analytics_refresh_interval_seconds == 30
    assert settings.poll_interval_seconds == 10
    assert settings.raw_write_interval_seconds == 10
    assert settings.derived_write_interval_seconds == 20
    assert settings.rollup_interval_seconds == 60
    assert settings.retention_days_raw == 30
    assert settings.retention_days_rollup == 180
    assert settings.cleanup_enabled is True


def test_runtime_timing_coordinator_separates_due_states():
    settings = RuntimeTimingSettings(
        analytics_refresh_interval_seconds=30,
        poll_interval_seconds=10,
        raw_write_interval_seconds=60,
        derived_write_interval_seconds=120,
        rollup_interval_seconds=300,
        persistence_enabled=True,
        live_refresh_enabled=True,
    )
    coordinator = RuntimeTimingCoordinator(
        settings,
        baseline={
            "last_successful_poll_utc": "2026-04-05T10:00:00Z",
            "last_successful_raw_write_utc": "2026-04-05T10:00:00Z",
            "last_successful_derived_write_utc": "2026-04-05T10:00:00Z",
            "last_successful_rollup_utc": "2026-04-05T10:00:00Z",
            "last_successful_kpi_summary_utc": "2026-04-05T10:00:00Z",
        },
    )
    plan = coordinator.plan_cycle("2026-04-05T10:01:10Z")
    assert plan["raw_write_due"] is True
    assert plan["derived_write_due"] is False
    assert plan["rollup_due"] is False
    assert plan["kpi_due"] is False


def test_runtime_timing_coordinator_tracks_success_markers():
    coordinator = RuntimeTimingCoordinator(RuntimeTimingSettings())
    coordinator.mark_poll("2026-04-05T10:00:00Z", success=True)
    coordinator.mark_raw_write("2026-04-05T10:00:10Z", success=True)
    coordinator.mark_derived_write("2026-04-05T10:00:20Z", success=True)
    coordinator.mark_rollup("2026-04-05T10:01:00Z", success=True)
    coordinator.mark_cleanup("2026-04-05T10:10:00Z", success=True)
    status = coordinator.get_status()
    assert status["last_successful_poll_utc"] == "2026-04-05T10:00:00Z"
    assert status["last_successful_raw_write_utc"] == "2026-04-05T10:00:10Z"
    assert status["last_successful_derived_write_utc"] == "2026-04-05T10:00:20Z"
    assert status["last_successful_rollup_utc"] == "2026-04-05T10:01:00Z"
    assert status["last_successful_cleanup_utc"] == "2026-04-05T10:10:00Z"


def test_build_runtime_hygiene_returns_structured_status():
    status = build_runtime_hygiene("127.0.0.1", 59999)
    assert status["host"] == "127.0.0.1"
    assert status["port"] == 59999
    assert "processes automatically" in status["process_hygiene_note"]
