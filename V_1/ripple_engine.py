from __future__ import annotations

from typing import Any, Dict


def estimate_ripple_effect(
    ctx: Any,
    eta_delta_hours: float,
    action_type: str,
    disruptor_event_count: int,
) -> Dict[str, float]:
    finance = ctx.payload.get("erp_finance", {})
    sla_prob = float(finance.get("sla_breach_probability_pct", "50") or 50)

    delay_hours = max(0.0, eta_delta_hours)
    labor_idle_cost = delay_hours * 80.0
    stockout_risk_pct = min(100.0, sla_prob + (delay_hours * 0.9) + (disruptor_event_count * 2.0))
    projected_lost_sales = (stockout_risk_pct / 100.0) * 50000.0

    mitigation_credit = 0.0
    if action_type in {"REROUTE_PORT", "EXPEDITE_TERMINAL_SLOT"}:
        mitigation_credit = 1500.0

    net_ripple_usd = labor_idle_cost + projected_lost_sales - mitigation_credit

    return {
        "labor_idle_cost_usd": round(labor_idle_cost, 2),
        "stockout_risk_pct": round(stockout_risk_pct, 2),
        "projected_lost_sales_usd": round(projected_lost_sales, 2),
        "mitigation_credit_usd": round(mitigation_credit, 2),
        "net_ripple_impact_usd": round(net_ripple_usd, 2),
    }
