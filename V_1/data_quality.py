from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


REQUIRED_FIELDS: Dict[str, List[str]] = {
    "tos_terminal": ["timestamp", "shipment_id", "terminal_id"],
    "tms_transport": ["timestamp", "shipment_id", "carrier_id", "delivery_delay_min"],
    "wms_warehouse": ["timestamp", "shipment_id", "dock_slot_availability"],
    "customs_compliance": ["timestamp", "shipment_id", "clearance_status"],
    "erp_finance": ["timestamp", "shipment_id", "sla_breach_probability_pct"],
    "logistics_visibility": ["timestamp", "shipment_id"],
    "iot_telemetry": ["timestamp", "shipment_id"],
}

MAX_AGE_HOURS_DEFAULT: Dict[str, float] = {
    "tos_terminal": 6.0,
    "tms_transport": 6.0,
    "wms_warehouse": 6.0,
    "customs_compliance": 12.0,
    "erp_finance": 12.0,
    "logistics_visibility": 6.0,
    "iot_telemetry": 6.0,
}


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def validate_source_rows(source_name: str, rows: List[Dict[str, str]]) -> List[str]:
    errors: List[str] = []

    if not rows:
        errors.append("No rows present")
        return errors

    required = REQUIRED_FIELDS.get(source_name, ["shipment_id"])
    headers = set(rows[0].keys())
    for col in required:
        if col not in headers:
            errors.append(f"Missing required column: {col}")

    shipment_missing = sum(1 for row in rows if not str(row.get("shipment_id", "")).strip())
    if shipment_missing > 0:
        errors.append(f"Rows missing shipment_id: {shipment_missing}")

    return errors


def validate_source_freshness(source_name: str, rows: List[Dict[str, str]]) -> List[str]:
    warnings: List[str] = []

    if not rows:
        return warnings
    if "timestamp" not in rows[0]:
        return warnings

    timestamps: List[datetime] = []
    for row in rows:
        value = str(row.get("timestamp", "")).strip()
        if not value:
            continue
        try:
            timestamps.append(_parse_utc(value))
        except ValueError:
            continue

    if not timestamps:
        warnings.append("No parseable timestamps found")
        return warnings

    newest = max(timestamps)
    age_hours = (datetime.now(timezone.utc) - newest).total_seconds() / 3600.0
    max_age = MAX_AGE_HOURS_DEFAULT.get(source_name, 12.0)
    if age_hours > max_age:
        warnings.append(f"Stale source: newest timestamp age {age_hours:.2f}h exceeds {max_age:.2f}h")

    return warnings


def source_health_summary(
    source_name: str,
    rows: List[Dict[str, str]],
) -> Dict[str, Any]:
    errors = validate_source_rows(source_name, rows)
    warnings = validate_source_freshness(source_name, rows)

    status = "healthy"
    if errors:
        status = "invalid"
    elif warnings:
        status = "degraded"

    return {
        "status": status,
        "errors": errors,
        "warnings": warnings,
        "row_count": len(rows),
    }
