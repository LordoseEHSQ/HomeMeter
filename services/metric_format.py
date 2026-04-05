from __future__ import annotations

from typing import Any


def format_metric_for_display(value: Any, unit: str | None) -> dict[str, str]:
    if value is None:
        return {"value": "-", "unit": unit or "-"}
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return {"value": str(value), "unit": unit or "-"}

    if unit == "W" and abs(numeric) >= 1000:
        return {"value": f"{numeric / 1000:.2f}", "unit": "kW"}
    if unit == "Wh" and abs(numeric) >= 1000:
        return {"value": f"{numeric / 1000:.2f}", "unit": "kWh"}
    if unit in {"V", "A", "Hz", "°C", "%", "VA", "VAr"}:
        decimals = 2 if unit in {"Hz", "%"} else 1
        return {"value": f"{numeric:.{decimals}f}", "unit": unit}
    return {"value": f"{numeric:.2f}", "unit": unit or "-"}
