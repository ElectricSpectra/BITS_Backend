from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List


@dataclass
class DisruptorImpact:
    event_id: str
    severity: str
    event_type: str
    reason: str
    hard_block: bool
    eta_penalty_hours: float
    cost_penalty_usd: float
    score_penalty: float


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _severity_weight(severity: str) -> float:
    lookup = {"CRITICAL": 1.0, "HIGH": 0.7, "MEDIUM": 0.4, "LOW": 0.2}
    return lookup.get(str(severity).upper(), 0.3)


def evaluate_route_disruptors(
    route_legs: List[Dict[str, str]],
    disruptors: List[Dict[str, str]],
    port_region_lookup: Dict[str, str],
    now_utc: datetime,
    horizon_days: int = 10,
) -> Dict[str, Any]:
    impacts: List[DisruptorImpact] = []
    horizon_end = now_utc.timestamp() + (horizon_days * 24 * 3600)

    for event in disruptors:
        try:
            start = _parse_utc(str(event.get("start_utc", ""))).timestamp()
            end = _parse_utc(str(event.get("end_utc", ""))).timestamp()
        except Exception:
            continue

        if end < now_utc.timestamp() or start > horizon_end:
            continue

        blocked_modes = {x.strip().upper() for x in str(event.get("blocked_modes", "")).split("|") if x.strip()}
        blocked_regions = {x.strip().upper() for x in str(event.get("blocked_regions", "")).split("|") if x.strip()}
        blocked_ports = {x.strip().upper() for x in str(event.get("blocked_ports", "")).split("|") if x.strip()}

        for leg in route_legs:
            mode = str(leg.get("mode", "")).upper()
            from_port = str(leg.get("from_port", "")).upper()
            to_port = str(leg.get("to_port", "")).upper()
            from_region = str(port_region_lookup.get(from_port, "")).upper()
            to_region = str(port_region_lookup.get(to_port, "")).upper()

            mode_hit = bool(blocked_modes and mode in blocked_modes)
            region_hit = bool(blocked_regions and (from_region in blocked_regions or to_region in blocked_regions))
            port_hit = bool(blocked_ports and (from_port in blocked_ports or to_port in blocked_ports))

            if not (mode_hit or region_hit or port_hit):
                continue

            sev = str(event.get("severity", "MEDIUM")).upper()
            w = _severity_weight(sev)
            hard_block = sev == "CRITICAL" and (mode_hit or port_hit)
            impacts.append(
                DisruptorImpact(
                    event_id=str(event.get("event_id", "UNKNOWN")),
                    severity=sev,
                    event_type=str(event.get("event_type", "DISRUPTION")),
                    reason=f"Disruptor hit on leg {leg.get('leg_id', '')} ({mode}/{from_port}->{to_port})",
                    hard_block=hard_block,
                    eta_penalty_hours=round(4.0 * w, 2),
                    cost_penalty_usd=round(120.0 * w, 2),
                    score_penalty=round(8.0 * w, 2),
                )
            )

    hard_block = any(item.hard_block for item in impacts)
    return {
        "hard_block": hard_block,
        "events": [item.__dict__ for item in impacts],
        "eta_penalty_hours": round(sum(item.eta_penalty_hours for item in impacts), 2),
        "cost_penalty_usd": round(sum(item.cost_penalty_usd for item in impacts), 2),
        "score_penalty": round(sum(item.score_penalty for item in impacts), 2),
    }
