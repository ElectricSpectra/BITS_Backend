from typing import Any, Dict, List, Tuple

from candidate_engine import CandidateAction


def score_candidate_components(action: CandidateAction, ctx: Any) -> Dict[str, float]:
    finance = ctx.payload.get("erp_finance", {})
    sla_prob = float(finance.get("sla_breach_probability_pct", "50") or 50)

    feasibility_component = 35.0 if action.feasibility else -40.0
    eta_component = max(-30.0, min(30.0, -action.eta_delta_hours * 8.0))
    cost_component = max(-20.0, min(20.0, -(action.cost_delta_usd / 80.0)))
    reliability_component = action.reliability_score * 20.0
    risk_relief_component = sla_prob * 0.25
    ripple = action.evidence.get("ripple_impact", {})
    ripple_impact_usd = float(ripple.get("net_ripple_impact_usd", 0.0) or 0.0)
    ripple_component = max(-25.0, min(5.0, -(ripple_impact_usd / 2500.0)))

    if action.action_type == "CUSTOMS_RESOLUTION":
        risk_relief_component += 10.0
    if action.action_type == "REROUTE_PORT":
        risk_relief_component += 8.0

    total_score = round(
        feasibility_component
        + eta_component
        + cost_component
        + reliability_component
        + risk_relief_component
        + ripple_component,
        2,
    )

    return {
        "feasibility_component": round(feasibility_component, 2),
        "eta_component": round(eta_component, 2),
        "cost_component": round(cost_component, 2),
        "reliability_component": round(reliability_component, 2),
        "risk_relief_component": round(risk_relief_component, 2),
        "ripple_component": round(ripple_component, 2),
        "total_score": total_score,
    }


def score_candidate(action: CandidateAction, ctx: Any) -> float:
    return score_candidate_components(action, ctx)["total_score"]


def rank_candidates(candidates: List[CandidateAction], ctx: Any) -> List[Tuple[CandidateAction, float]]:
    scored = [(candidate, score_candidate(candidate, ctx)) for candidate in candidates]
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored


def candidate_to_dict(action: CandidateAction, score: float) -> Dict[str, Any]:
    return {
        "action_id": action.action_id,
        "action_type": action.action_type,
        "summary": action.summary,
        "from_port": action.from_port,
        "to_port": action.to_port,
        "carrier_id": action.carrier_id,
        "lane_ids": action.lane_ids,
        "leg_ids": action.leg_ids,
        "eta_delta_hours": action.eta_delta_hours,
        "cost_delta_usd": action.cost_delta_usd,
        "reliability_score": action.reliability_score,
        "feasibility": action.feasibility,
        "blocked_reasons": action.blocked_reasons,
        "mode_breakdown": action.evidence.get("mode_breakdown", ""),
        "owner": action.owner,
        "due_by": action.due_by,
        "score": score,
        "evidence": action.evidence,
    }
