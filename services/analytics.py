from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from services.i18n import translate
from services.metric_format import format_metric_for_display
from services.time_utils import TimeSettings, format_timestamp_for_display
from services.time_utils import normalize_storage_timestamp, parse_utc_timestamp
from storage.sqlite_store import KPIRecord, MinuteRollupRecord, SemanticMetricRecord, SQLiteStore


WINDOWS: dict[str, dict[str, Any]] = {
    "24h": {"label_key": "window.24h", "delta": timedelta(hours=24)},
    "7d": {"label_key": "window.7d", "delta": timedelta(days=7)},
    "4w": {"label_key": "window.4w", "delta": timedelta(weeks=4)},
    "quarter": {"label_key": "window.quarter", "delta": timedelta(days=91)},
    "6m": {"label_key": "window.6m", "delta": timedelta(days=183)},
    "today": {"label_key": "window.today", "start_of_day": True},
    "yesterday": {"label_key": "window.yesterday", "yesterday": True},
    "month": {"label_key": "window.month", "start_of_month": True},
}

KPI_DEFINITIONS = {
    "pv_generation_kwh": {"label_key": "metric.pv_generation_kwh", "unit": "kWh"},
    "house_consumption_kwh": {"label_key": "metric.house_consumption_kwh", "unit": "kWh"},
    "house_consumption_without_wallbox_kwh": {"label_key": "metric.house_consumption_without_wallbox_kwh", "unit": "kWh"},
    "grid_import_kwh": {"label_key": "metric.grid_import_kwh", "unit": "kWh"},
    "grid_export_kwh": {"label_key": "metric.grid_export_kwh", "unit": "kWh"},
    "wallbox_energy_kwh": {"label_key": "metric.wallbox_energy_kwh", "unit": "kWh"},
    "self_consumption_kwh": {"label_key": "metric.self_consumption_kwh", "unit": "kWh"},
    "self_consumption_ratio": {"label_key": "metric.self_consumption_ratio", "unit": "%"},
    "self_sufficiency_ratio": {"label_key": "metric.self_sufficiency_ratio", "unit": "%"},
    "wallbox_share": {"label_key": "metric.wallbox_share", "unit": "%"},
    "average_house_consumption_w": {"label_key": "metric.average_house_consumption_w", "unit": "W"},
    "peak_house_consumption_w": {"label_key": "metric.peak_house_consumption_w", "unit": "W"},
    "peak_pv_power_w": {"label_key": "metric.peak_pv_power_w", "unit": "W"},
}

KPI_INTERPRETATION = [
    {
        "title_key": "analytics.interpretation.self_consumption.title",
        "text_key": "analytics.interpretation.self_consumption.text",
    },
    {
        "title_key": "analytics.interpretation.self_sufficiency.title",
        "text_key": "analytics.interpretation.self_sufficiency.text",
    },
    {
        "title_key": "analytics.interpretation.wallbox.title",
        "text_key": "analytics.interpretation.wallbox.text",
    },
    {
        "title_key": "analytics.interpretation.house_without_wallbox.title",
        "text_key": "analytics.interpretation.house_without_wallbox.text",
    },
]

CHART_METRICS = [
    ("pv_power_w", "metric.pv_power_w"),
    ("house_consumption_w", "metric.house_consumption_w"),
    ("grid_net_power_w", "metric.grid_net_power_w"),
    ("wallbox_power_w", "metric.wallbox_power_w"),
]

MEASURED_SOURCE_TYPES = {"confirmed_useful", "verified"}
ESTIMATED_SOURCE_TYPES = {"tentative", "likely_useful_candidate", "unmapped_numeric", "raw_numeric"}


@dataclass(slots=True)
class LatestMetric:
    metric_name: str
    value: float
    unit: str | None
    source_type: str
    timestamp_utc: str
    device_name: str


class AnalyticsEngine:
    def __init__(self, store: SQLiteStore, settings: dict[str, Any] | None = None) -> None:
        self.store = store
        self.settings = settings or {}
        self.formula_version = "analytics_v1"

    def process_cycle(self, timestamp_utc: str) -> dict[str, Any]:
        semantic_metrics = self.build_semantic_metrics(timestamp_utc)
        if semantic_metrics:
            self.persist_semantic_metrics(semantic_metrics)
            bucket_utc = floor_minute_bucket(timestamp_utc)
            metric_names = sorted({metric.metric_name for metric in semantic_metrics})
            self.refresh_rollups(timestamp_utc, metric_names=metric_names)
        else:
            bucket_utc = floor_minute_bucket(timestamp_utc)
        kpi_records = self.build_kpi_records(timestamp_utc)
        if kpi_records:
            self.persist_kpi_records(kpi_records)
        return {
            "timestamp_utc": normalize_storage_timestamp(timestamp_utc),
            "semantic_metric_count": len(semantic_metrics),
            "rollup_bucket_utc": bucket_utc,
            "kpi_record_count": len(kpi_records),
        }

    def persist_semantic_metrics(self, records: list[SemanticMetricRecord]) -> None:
        self.store.save_semantic_metrics(records)

    def refresh_rollups(self, timestamp_utc: str, metric_names: Iterable[str] | None = None) -> list[MinuteRollupRecord]:
        names = sorted(set(metric_names or self.semantic_metric_names()))
        return self.store.refresh_minute_rollups(
            bucket_utc=floor_minute_bucket(timestamp_utc),
            metric_names=names,
            device_name="system",
        )

    def persist_kpi_records(self, records: list[KPIRecord]) -> None:
        self.store.upsert_kpi_summaries(records)

    def semantic_metric_names(self) -> list[str]:
        return [
            "pv_power_w",
            "grid_import_w",
            "grid_export_w",
            "grid_net_power_w",
            "wallbox_power_w",
            "house_consumption_w",
            "house_consumption_without_wallbox_w",
        ]

    def build_dashboard(
        self,
        selected_window: str = "24h",
        *,
        language: str = "en",
        time_settings: TimeSettings | None = None,
    ) -> dict[str, Any]:
        window_key = selected_window if selected_window in WINDOWS else self.settings.get("default_window", "24h")
        now_utc = datetime.now(timezone.utc)
        start_utc, end_utc = resolve_window(window_key, now_utc)
        kpi_rows = {row["metric_name"]: row for row in self.store.get_kpi_summaries(window_key)}
        coverage = self._build_coverage_summary(window_key, language)
        chart_series = [
            self._build_chart(
                metric_name,
                label_key,
                start_utc,
                end_utc,
                language=language,
                time_settings=time_settings,
            )
            for metric_name, label_key in CHART_METRICS
        ]
        cards = [
            self._card_from_kpi(kpi_rows, "self_consumption_ratio", language),
            self._card_from_kpi(kpi_rows, "self_sufficiency_ratio", language),
            self._card_from_kpi(kpi_rows, "house_consumption_kwh", language),
            self._card_from_kpi(kpi_rows, "pv_generation_kwh", language),
            self._card_from_kpi(kpi_rows, "grid_import_kwh", language),
            self._card_from_kpi(kpi_rows, "grid_export_kwh", language),
            self._card_from_kpi(kpi_rows, "wallbox_energy_kwh", language),
        ]
        table_metrics = [
            self._card_from_kpi(kpi_rows, metric_name, language)
            for metric_name in KPI_DEFINITIONS
        ]
        primary_metrics = {
            "energy": [
                self._card_from_kpi(kpi_rows, metric_name, language)
                for metric_name in [
                    "pv_generation_kwh",
                    "house_consumption_kwh",
                    "grid_import_kwh",
                    "grid_export_kwh",
                    "wallbox_energy_kwh",
                    "house_consumption_without_wallbox_kwh",
                ]
            ],
            "ratios": [
                self._card_from_kpi(kpi_rows, metric_name, language)
                for metric_name in [
                    "self_consumption_ratio",
                    "self_sufficiency_ratio",
                    "wallbox_share",
                    "average_house_consumption_w",
                    "peak_house_consumption_w",
                    "peak_pv_power_w",
                ]
            ],
        }
        return {
            "selected_window": window_key,
            "window_options": [{"key": key, "label": translate(value["label_key"], language)} for key, value in WINDOWS.items()],
            "window_label": translate(WINDOWS[window_key]["label_key"], language),
            "window_start_utc": normalize_storage_timestamp(start_utc),
            "window_end_utc": normalize_storage_timestamp(end_utc),
            "cards": cards,
            "primary_metrics": primary_metrics,
            "all_kpis": table_metrics,
            "interpretation": [
                {
                    "title": translate(item["title_key"], language),
                    "text": translate(item["text_key"], language),
                }
                for item in KPI_INTERPRETATION
            ],
            "chart_series": chart_series,
            "coverage": coverage,
            "persistence_status": self.store.get_analytics_status(),
            "refresh_seconds": int(self.settings.get("chart_refresh_seconds", 30)),
        }

    def build_live_summary(self, collector_results: list[Any], *, language: str = "en") -> dict[str, Any]:
        if not collector_results:
            return {"timestamp_utc": None, "metric_count": 0, "items": []}
        latest = self.build_latest_measurement_map(collector_results)
        newest_timestamp = max((result.timestamp_utc for result in collector_results), default=None)
        semantic_metrics = self.build_semantic_metrics(newest_timestamp or utc_timestamp_now(), latest_measurements=latest)
        items = []
        for record in semantic_metrics:
            items.append(
                {
                    "metric_name": record.metric_name,
                    "label": translate(f"metric.{record.metric_name}", language),
                    "value": record.metric_value,
                    "unit": record.unit,
                    "classification": record.classification,
                    "confidence_state": record.confidence_state,
                    "source_coverage": record.source_coverage,
                }
            )
        return {
            "timestamp_utc": newest_timestamp,
            "metric_count": len(items),
            "items": sorted(items, key=lambda item: item["label"]),
        }

    def build_kpi_records(self, timestamp_utc: str) -> list[KPIRecord]:
        now_utc = parse_utc_timestamp(timestamp_utc) or datetime.now(timezone.utc)
        records: list[KPIRecord] = []
        for window_key in WINDOWS:
            start_utc, end_utc = resolve_window(window_key, now_utc)
            metrics = self._compute_window_metrics(window_key, start_utc, end_utc, timestamp_utc)
            records.extend(metrics)
        return records

    def build_semantic_metrics(
        self,
        timestamp_utc: str,
        latest_measurements: dict[str, LatestMetric] | None = None,
    ) -> list[SemanticMetricRecord]:
        if latest_measurements is None:
            latest = {
                name: self._latest_metric(name)
                for name in [
                    "pv_power_w",
                    "inverter_ac_power_w",
                    "grid_power_w",
                    "house_power_w",
                    "wallbox_power_w",
                ]
            }
        else:
            latest = latest_measurements
        records: list[SemanticMetricRecord] = []

        grid_metric = latest["grid_power_w"]
        if grid_metric:
            net_class, net_confidence = classify_source_type(grid_metric.source_type)
            records.append(
                self._semantic_record(
                    timestamp_utc=timestamp_utc,
                    metric_name="grid_net_power_w",
                    value=grid_metric.value,
                    unit="W",
                    classification=net_class,
                    confidence_state=net_confidence,
                    source_metric_names=[grid_metric.metric_name],
                    source_coverage=1.0,
                    note="Direct semantic grid net power from latest grid_power_w measurement.",
                )
            )
            records.append(
                self._semantic_record(
                    timestamp_utc=timestamp_utc,
                    metric_name="grid_import_w",
                    value=max(grid_metric.value, 0.0),
                    unit="W",
                    classification="derived",
                    confidence_state=net_confidence,
                    source_metric_names=[grid_metric.metric_name],
                    source_coverage=1.0,
                    note="Positive component of grid net power.",
                )
            )
            records.append(
                self._semantic_record(
                    timestamp_utc=timestamp_utc,
                    metric_name="grid_export_w",
                    value=max(-grid_metric.value, 0.0),
                    unit="W",
                    classification="derived",
                    confidence_state=net_confidence,
                    source_metric_names=[grid_metric.metric_name],
                    source_coverage=1.0,
                    note="Negative component of grid net power stored as export.",
                )
            )

        house_metric = latest["house_power_w"]
        if house_metric:
            classification, confidence = classify_source_type(house_metric.source_type)
            records.append(
                self._semantic_record(
                    timestamp_utc=timestamp_utc,
                    metric_name="house_consumption_w",
                    value=house_metric.value,
                    unit="W",
                    classification=classification,
                    confidence_state=confidence,
                    source_metric_names=[house_metric.metric_name],
                    source_coverage=1.0,
                    note="Direct semantic house consumption from latest house_power_w measurement.",
                )
            )

        wallbox_metric = latest["wallbox_power_w"]
        if wallbox_metric:
            classification, confidence = classify_source_type(wallbox_metric.source_type)
            records.append(
                self._semantic_record(
                    timestamp_utc=timestamp_utc,
                    metric_name="wallbox_power_w",
                    value=wallbox_metric.value,
                    unit="W",
                    classification=classification,
                    confidence_state=confidence,
                    source_metric_names=[wallbox_metric.metric_name],
                    source_coverage=1.0,
                    note="Direct semantic wallbox power from latest wallbox metric.",
                )
            )

        pv_metric = latest["pv_power_w"]
        if pv_metric:
            classification, confidence = classify_source_type(pv_metric.source_type)
            records.append(
                self._semantic_record(
                    timestamp_utc=timestamp_utc,
                    metric_name="pv_power_w",
                    value=max(pv_metric.value, 0.0),
                    unit="W",
                    classification=classification,
                    confidence_state=confidence,
                    source_metric_names=[pv_metric.metric_name],
                    source_coverage=1.0,
                    note="Direct semantic PV power from latest pv_power_w measurement.",
                )
            )
        elif latest["inverter_ac_power_w"]:
            inverter_metric = latest["inverter_ac_power_w"]
            records.append(
                self._semantic_record(
                    timestamp_utc=timestamp_utc,
                    metric_name="pv_power_w",
                    value=max(inverter_metric.value, 0.0),
                    unit="W",
                    classification="estimated",
                    confidence_state="tentative",
                    source_metric_names=[inverter_metric.metric_name],
                    source_coverage=1.0,
                    note="Estimated PV power from KOSTAL inverter_ac_power_w until verified plant-level PV mapping is confirmed.",
                )
            )

        semantic_map = {record.metric_name: record for record in records}
        if "house_consumption_w" not in semantic_map:
            pv_record = semantic_map.get("pv_power_w")
            import_record = semantic_map.get("grid_import_w")
            export_record = semantic_map.get("grid_export_w")
            components = [item for item in [pv_record, import_record, export_record] if item]
            if len(components) == 3:
                value = max(pv_record.metric_value + import_record.metric_value - export_record.metric_value, 0.0)
                records.append(
                    self._semantic_record(
                        timestamp_utc=timestamp_utc,
                        metric_name="house_consumption_w",
                        value=value,
                        unit="W",
                        classification="derived",
                        confidence_state=combine_confidence([item.confidence_state for item in components]),
                        source_metric_names=[item.metric_name for item in components],
                        source_coverage=1.0,
                        note="Derived house consumption from PV plus grid import minus grid export.",
                    )
                )
                semantic_map["house_consumption_w"] = records[-1]

        if "house_consumption_w" in semantic_map and "wallbox_power_w" in semantic_map:
            house_record = semantic_map["house_consumption_w"]
            wallbox_record = semantic_map["wallbox_power_w"]
            records.append(
                self._semantic_record(
                    timestamp_utc=timestamp_utc,
                    metric_name="house_consumption_without_wallbox_w",
                    value=max(house_record.metric_value - wallbox_record.metric_value, 0.0),
                    unit="W",
                    classification="derived",
                    confidence_state=combine_confidence([house_record.confidence_state, wallbox_record.confidence_state]),
                    source_metric_names=[house_record.metric_name, wallbox_record.metric_name],
                    source_coverage=1.0,
                    note="Derived house consumption without EV charging load.",
                )
            )
        return records

    def build_latest_measurement_map(self, collector_results: list[Any]) -> dict[str, LatestMetric]:
        latest: dict[str, LatestMetric] = {}
        for result in collector_results:
            for measurement in result.measurements:
                latest[measurement.metric_name] = LatestMetric(
                    metric_name=measurement.metric_name,
                    value=float(measurement.metric_value),
                    unit=measurement.unit,
                    source_type=measurement.source_type,
                    timestamp_utc=result.timestamp_utc,
                    device_name=result.device_name,
                )
        return latest

    def _latest_metric(self, metric_name: str) -> LatestMetric | None:
        row = self.store.get_latest_measurement_row(metric_name)
        if not row:
            return None
        return LatestMetric(
            metric_name=row["metric_name"],
            value=float(row["metric_value"]),
            unit=row.get("unit"),
            source_type=str(row.get("source_type", "")),
            timestamp_utc=row["timestamp_utc"],
            device_name=row["device_name"],
        )

    def _semantic_record(
        self,
        *,
        timestamp_utc: str,
        metric_name: str,
        value: float,
        unit: str | None,
        classification: str,
        confidence_state: str,
        source_metric_names: list[str],
        source_coverage: float,
        note: str,
    ) -> SemanticMetricRecord:
        return SemanticMetricRecord(
            timestamp_utc=normalize_storage_timestamp(timestamp_utc) or timestamp_utc,
            device_name="system",
            metric_name=metric_name,
            metric_value=round(float(value), 6),
            unit=unit,
            classification=classification,
            formula_version=self.formula_version,
            source_metric_names=source_metric_names,
            source_coverage=round(float(source_coverage), 3),
            confidence_state=confidence_state,
            details={"note": note},
        )

    def _compute_window_metrics(
        self,
        window_key: str,
        start_utc: datetime,
        end_utc: datetime,
        updated_at_utc: str,
    ) -> list[KPIRecord]:
        power_metrics = {
            "pv_power_w": self.store.aggregate_rollup_window("pv_power_w", start_utc, end_utc),
            "grid_import_w": self.store.aggregate_rollup_window("grid_import_w", start_utc, end_utc),
            "grid_export_w": self.store.aggregate_rollup_window("grid_export_w", start_utc, end_utc),
            "wallbox_power_w": self.store.aggregate_rollup_window("wallbox_power_w", start_utc, end_utc),
            "house_consumption_w": self.store.aggregate_rollup_window("house_consumption_w", start_utc, end_utc),
            "house_consumption_without_wallbox_w": self.store.aggregate_rollup_window("house_consumption_without_wallbox_w", start_utc, end_utc),
        }
        records: list[KPIRecord] = []

        energy_sources = {
            "pv_generation_kwh": "pv_power_w",
            "grid_import_kwh": "grid_import_w",
            "grid_export_kwh": "grid_export_w",
            "wallbox_energy_kwh": "wallbox_power_w",
            "house_consumption_kwh": "house_consumption_w",
            "house_consumption_without_wallbox_kwh": "house_consumption_without_wallbox_w",
        }
        energy = {
            metric_name: power_metrics[source_metric]["energy_kwh"]
            for metric_name, source_metric in energy_sources.items()
        }

        for metric_name, value in energy.items():
            aggregate = power_metrics[energy_sources[metric_name]]
            if value is None:
                continue
            records.append(
                self._kpi_record(
                    window_key,
                    start_utc,
                    end_utc,
                    metric_name,
                    value,
                    "kWh",
                    classification=aggregate["classification"],
                    confidence_state=aggregate["confidence_state"],
                    source_coverage=aggregate["coverage"],
                    updated_at_utc=updated_at_utc,
                    details={"sample_count": aggregate["sample_count"], "expected_count": aggregate["expected_count"]},
                )
            )

        derived_formulas = [
            ("self_consumption_kwh", self._safe_subtract(energy["pv_generation_kwh"], energy["grid_export_kwh"]), ["pv_generation_kwh", "grid_export_kwh"]),
        ]
        for metric_name, value, inputs in derived_formulas:
            if value is None:
                continue
            coverages = [self._kpi_coverage(records, input_name) for input_name in inputs]
            confidence_inputs = [self._kpi_confidence(records, input_name) for input_name in inputs if self._kpi_confidence(records, input_name)]
            coverage = min(coverages) if coverages else 0.0
            confidence = combine_confidence(confidence_inputs)
            records.append(
                self._kpi_record(
                    window_key,
                    start_utc,
                    end_utc,
                    metric_name,
                    value,
                    "kWh",
                    classification="derived",
                    confidence_state=confidence,
                    source_coverage=coverage,
                    updated_at_utc=updated_at_utc,
                    details={"inputs": inputs},
                )
            )

        ratios = [
            ("self_consumption_ratio", safe_ratio(self._kpi_value(records, "self_consumption_kwh"), self._kpi_value(records, "pv_generation_kwh")), ["self_consumption_kwh", "pv_generation_kwh"]),
            ("self_sufficiency_ratio", self._safe_self_sufficiency(self._kpi_value(records, "grid_import_kwh"), self._kpi_value(records, "house_consumption_kwh")), ["grid_import_kwh", "house_consumption_kwh"]),
            ("wallbox_share", safe_ratio(self._kpi_value(records, "wallbox_energy_kwh"), self._kpi_value(records, "house_consumption_kwh")), ["wallbox_energy_kwh", "house_consumption_kwh"]),
        ]
        for metric_name, ratio, inputs in ratios:
            if ratio is None:
                continue
            coverages = [self._kpi_coverage(records, input_name) for input_name in inputs]
            confidence_inputs = [self._kpi_confidence(records, input_name) for input_name in inputs if self._kpi_confidence(records, input_name)]
            coverage = min(coverages) if coverages else 0.0
            confidence = combine_confidence(confidence_inputs)
            records.append(
                self._kpi_record(
                    window_key,
                    start_utc,
                    end_utc,
                    metric_name,
                    ratio * 100.0,
                    "%",
                    classification="derived",
                    confidence_state=confidence,
                    source_coverage=coverage,
                    updated_at_utc=updated_at_utc,
                    details={"inputs": inputs},
                )
            )

        scalar_metrics = [
            ("average_house_consumption_w", power_metrics["house_consumption_w"]["average_power_w"], "W", power_metrics["house_consumption_w"]),
            ("peak_house_consumption_w", power_metrics["house_consumption_w"]["peak_power_w"], "W", power_metrics["house_consumption_w"]),
            ("peak_pv_power_w", power_metrics["pv_power_w"]["peak_power_w"], "W", power_metrics["pv_power_w"]),
        ]
        for metric_name, value, unit, aggregate in scalar_metrics:
            if value is None:
                continue
            records.append(
                self._kpi_record(
                    window_key,
                    start_utc,
                    end_utc,
                    metric_name,
                    value,
                    unit,
                    classification=aggregate["classification"],
                    confidence_state=aggregate["confidence_state"],
                    source_coverage=aggregate["coverage"],
                    updated_at_utc=updated_at_utc,
                    details={"sample_count": aggregate["sample_count"], "expected_count": aggregate["expected_count"]},
                )
            )
        return records

    def _kpi_record(
        self,
        window_key: str,
        start_utc: datetime,
        end_utc: datetime,
        metric_name: str,
        value: float,
        unit: str,
        *,
        classification: str,
        confidence_state: str,
        source_coverage: float,
        updated_at_utc: str,
        details: dict[str, Any],
    ) -> KPIRecord:
        effective_classification = classification
        if source_coverage < 0.5 and effective_classification != "estimated":
            effective_classification = "estimated"
        effective_confidence = confidence_state if source_coverage >= 0.8 else "incomplete"
        return KPIRecord(
            window_key=window_key,
            window_start_utc=normalize_storage_timestamp(start_utc) or str(start_utc),
            window_end_utc=normalize_storage_timestamp(end_utc) or str(end_utc),
            metric_name=metric_name,
            metric_value=round(float(value), 6),
            unit=unit,
            classification=effective_classification,
            formula_version=self.formula_version,
            source_coverage=round(float(source_coverage), 3),
            confidence_state=effective_confidence,
            updated_at_utc=normalize_storage_timestamp(updated_at_utc) or updated_at_utc,
            details=details,
        )

    def _card_from_kpi(self, rows: dict[str, dict[str, Any]], metric_name: str, language: str) -> dict[str, Any]:
        definition = KPI_DEFINITIONS[metric_name]
        row = rows.get(metric_name)
        if not row:
            return {
                "metric_name": metric_name,
                "label": translate(definition["label_key"], language),
                "value": None,
                "unit": definition["unit"],
                "classification": "unavailable",
                "confidence_state": "not_enough_data",
                "source_coverage": 0.0,
                "message": translate("common.not_enough_data", language),
            }
        return {
            "metric_name": metric_name,
            "label": translate(definition["label_key"], language),
            "value": row["metric_value"],
            "unit": row["unit"],
            "classification": row["classification"],
            "confidence_state": row["confidence_state"],
            "source_coverage": row["source_coverage"],
            "message": self._coverage_message(row["source_coverage"], row["classification"], language),
        }

    def _build_chart(
        self,
        metric_name: str,
        label_key: str,
        start_utc: datetime,
        end_utc: datetime,
        *,
        language: str,
        time_settings: TimeSettings | None,
    ) -> dict[str, Any]:
        series = self.store.get_rollup_series(metric_name, start_utc, end_utc, limit=240)
        if len(series) < 2:
            return {
                "metric_name": metric_name,
                "label": translate(label_key, language),
                "empty": True,
                "message": translate("analytics.chart.no_data", language),
            }

        raw_values = [float(item["avg_value"]) for item in series]
        raw_unit = str(series[-1].get("unit") or "")
        scale = choose_chart_scale(raw_values, raw_unit)
        scaled_values = [value * scale["multiplier"] for value in raw_values]
        chart_geometry = {"width": 840, "height": 280, "padding_left": 68, "padding_right": 24, "padding_top": 20, "padding_bottom": 44}
        points = build_svg_points(
            scaled_values,
            width=chart_geometry["width"] - chart_geometry["padding_left"] - chart_geometry["padding_right"],
            height=chart_geometry["height"] - chart_geometry["padding_top"] - chart_geometry["padding_bottom"],
            x_offset=chart_geometry["padding_left"],
            y_offset=chart_geometry["padding_top"],
        )
        y_ticks = build_chart_y_ticks(
            scaled_values,
            unit=scale["display_unit"],
            height=chart_geometry["height"],
            padding_top=chart_geometry["padding_top"],
            padding_bottom=chart_geometry["padding_bottom"],
        )
        x_ticks = build_chart_x_ticks(
            series,
            width=chart_geometry["width"],
            padding_left=chart_geometry["padding_left"],
            padding_right=chart_geometry["padding_right"],
            time_settings=time_settings,
        )
        return {
            "metric_name": metric_name,
            "label": translate(label_key, language),
            "empty": False,
            "points": points,
            "chart_width": chart_geometry["width"],
            "chart_height": chart_geometry["height"],
            "baseline_y": chart_geometry["height"] - chart_geometry["padding_bottom"],
            "plot_left": chart_geometry["padding_left"],
            "plot_right": chart_geometry["width"] - chart_geometry["padding_right"],
            "plot_top": chart_geometry["padding_top"],
            "latest_value": series[-1]["last_value"],
            "latest_unit": series[-1]["unit"],
            "latest_bucket_utc": series[-1]["bucket_utc"],
            "series_count": len(series),
            "y_ticks": y_ticks,
            "x_ticks": x_ticks,
            "display_unit": scale["display_unit"],
            "range_min": min(scaled_values),
            "range_max": max(scaled_values),
            "average_display_value": sum(scaled_values) / len(scaled_values),
        }

    def _build_coverage_summary(self, window_key: str, language: str) -> dict[str, Any]:
        rows = self.store.get_kpi_summaries(window_key)
        if not rows:
            return {"status": "warning", "message": translate("status.coverage.empty", language)}
        minimum = min(float(row["source_coverage"] or 0.0) for row in rows)
        if minimum >= 0.8:
            status = "healthy"
            message = translate("status.coverage.healthy", language)
        elif minimum >= 0.5:
            status = "warning"
            message = translate("status.coverage.warning", language)
        else:
            status = "partial"
            message = translate("status.coverage.partial", language)
        return {"status": status, "message": message}

    def _coverage_message(self, coverage: float, classification: str, language: str) -> str:
        label = translate(f"classification.{classification}", language)
        if coverage >= 0.8:
            return f"{label}, {translate('analytics.coverage.strong_source', language)}"
        if coverage > 0:
            return f"{label}, {translate('analytics.coverage.partial_source', language)}"
        return translate("common.not_enough_data", language)

    def _kpi_value(self, rows: list[KPIRecord], metric_name: str) -> float | None:
        for row in rows:
            if row.metric_name == metric_name:
                return row.metric_value
        return None

    def _kpi_coverage(self, rows: list[KPIRecord], metric_name: str) -> float:
        for row in rows:
            if row.metric_name == metric_name:
                return row.source_coverage
        return 0.0

    def _kpi_confidence(self, rows: list[KPIRecord], metric_name: str) -> str | None:
        for row in rows:
            if row.metric_name == metric_name:
                return row.confidence_state
        return None

    def _safe_subtract(self, left: float | None, right: float | None) -> float | None:
        if left is None or right is None:
            return None
        return max(left - right, 0.0)

    def _safe_self_sufficiency(self, grid_import_kwh: float | None, house_consumption_kwh: float | None) -> float | None:
        if grid_import_kwh is None or house_consumption_kwh in {None, 0}:
            return None
        return max(0.0, min(1.0, 1.0 - (grid_import_kwh / house_consumption_kwh)))


def floor_minute_bucket(timestamp_utc: str | datetime) -> str:
    parsed = parse_utc_timestamp(timestamp_utc) if isinstance(timestamp_utc, str) else timestamp_utc.astimezone(timezone.utc)
    if parsed is None:
        parsed = datetime.now(timezone.utc)
    return parsed.replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_window(window_key: str, now_utc: datetime) -> tuple[datetime, datetime]:
    config = WINDOWS[window_key]
    end_utc = now_utc.astimezone(timezone.utc).replace(microsecond=0)
    if config.get("start_of_day"):
        start_utc = end_utc.replace(hour=0, minute=0, second=0)
    elif config.get("yesterday"):
        today = end_utc.replace(hour=0, minute=0, second=0)
        start_utc = today - timedelta(days=1)
        end_utc = today
    elif config.get("start_of_month"):
        start_utc = end_utc.replace(day=1, hour=0, minute=0, second=0)
    else:
        start_utc = end_utc - config["delta"]
    return start_utc, end_utc


def classify_source_type(source_type: str) -> tuple[str, str]:
    if source_type in MEASURED_SOURCE_TYPES:
        return "measured", "high"
    if source_type in ESTIMATED_SOURCE_TYPES:
        return "estimated", "tentative"
    return "measured", "medium"


def combine_confidence(states: list[str]) -> str:
    if not states:
        return "unknown"
    if "incomplete" in states:
        return "incomplete"
    if "tentative" in states:
        return "tentative"
    if "medium" in states:
        return "medium"
    if "high" in states:
        return "high"
    return states[0]


def safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in {None, 0}:
        return None
    value = numerator / denominator
    return max(0.0, min(1.0, value))


def build_svg_points(
    values: list[float],
    width: int,
    height: int,
    *,
    x_offset: float = 0.0,
    y_offset: float = 0.0,
) -> str:
    if not values:
        return ""
    minimum = min(values)
    maximum = max(values)
    span = maximum - minimum or 1.0
    x_step = width / max(len(values) - 1, 1)
    points = []
    for index, value in enumerate(values):
        x = round(x_offset + (index * x_step), 2)
        normalized = (value - minimum) / span
        y = round(y_offset + height - (normalized * height), 2)
        points.append(f"{x},{y}")
    return " ".join(points)


def choose_chart_scale(values: list[float], unit: str) -> dict[str, Any]:
    max_abs = max((abs(value) for value in values), default=0.0)
    if unit == "W" and max_abs >= 1000:
        return {"display_unit": "kW", "multiplier": 0.001}
    if unit == "Wh" and max_abs >= 1000:
        return {"display_unit": "kWh", "multiplier": 0.001}
    return {"display_unit": unit or "-", "multiplier": 1.0}


def build_chart_y_ticks(
    values: list[float],
    *,
    unit: str,
    height: int,
    padding_top: int,
    padding_bottom: int,
    tick_count: int = 4,
) -> list[dict[str, Any]]:
    minimum = min(values)
    maximum = max(values)
    if minimum == maximum:
        maximum = minimum + 1.0
    span = maximum - minimum
    plot_height = height - padding_top - padding_bottom
    ticks: list[dict[str, Any]] = []
    for index in range(tick_count):
        ratio = index / max(tick_count - 1, 1)
        value = maximum - (span * ratio)
        y = padding_top + (plot_height * ratio)
        formatted = format_metric_for_display(value, unit)
        ticks.append({"y": round(y, 2), "label": f"{formatted['value']} {formatted['unit']}".strip()})
    return ticks


def build_chart_x_ticks(
    series: list[dict[str, Any]],
    *,
    width: int,
    padding_left: int,
    padding_right: int,
    time_settings: TimeSettings | None,
    tick_count: int = 5,
) -> list[dict[str, Any]]:
    if not series:
        return []
    plot_width = width - padding_left - padding_right
    steps = max(min(tick_count, len(series)) - 1, 1)
    ticks: list[dict[str, Any]] = []
    for index in range(steps + 1):
        series_index = round(index * (len(series) - 1) / steps)
        point = series[series_index]
        x = padding_left + (plot_width * index / steps)
        label = point["bucket_utc"]
        if time_settings is not None:
            formatted = format_timestamp_for_display(point["bucket_utc"], time_settings)
            label = formatted[11:19] if len(formatted) >= 19 else formatted
        ticks.append({"x": round(x, 2), "label": label, "timestamp_utc": point["bucket_utc"]})
    return ticks


def utc_timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
