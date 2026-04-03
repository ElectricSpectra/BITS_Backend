import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any

from graph_engine import LogisticsGraph
from ripple_engine import estimate_ripple_effect


@dataclass
class CandidateAction:
    action_id: str
    action_type: str
    summary: str
    from_port: str
    to_port: str
    carrier_id: str
    lane_ids: List[str]
    leg_ids: List[str]
    eta_delta_hours: float
    cost_delta_usd: float
    reliability_score: float
    feasibility: bool
    blocked_reasons: List[str]
    owner: str
    due_by: str
    evidence: Dict[str, Any]


def generate_candidate_actions(
    ctx: Any,
    graph: LogisticsGraph,
    now_utc: datetime,
    scenario: Dict[str, Any] | None = None,
) -> List[CandidateAction]:
    shipment_id = ctx.shipment_id
    payload = ctx.payload
    scenario = scenario or {}

    current_port = graph.get_current_port(payload) or "UNKNOWN"
    commitment = graph.get_commitment(shipment_id)
    destination_port = graph.get_destination_port(shipment_id) or current_port
    region = payload.get("tos_terminal", {}).get("region", "APAC")

    customs = payload.get("customs_compliance", {})
    tms = payload.get("tms_transport", {})
    finance = payload.get("erp_finance", {})
    tos = payload.get("tos_terminal", {})

    due_by = (now_utc + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    max_hops = int(os.getenv("ROUTE_MAX_HOPS", "2"))
    plans = graph.enumerate_route_plans(
        current_port,
        destination_port,
        region=region,
        max_hops=max_hops,
        scenario=scenario,
    )
    candidates: List[CandidateAction] = []

    base_sla_prob = float(finance.get("sla_breach_probability_pct", "50") or 50)
    delivery_delay = float(tms.get("delivery_delay_min", "0") or 0)

    scenario_delay = float(scenario.get("delay_hours", 0) or 0)
    scenario_wait = float(scenario.get("wait_hours", 0) or 0)
    scenario_preferred_carrier = str(scenario.get("preferred_carrier", "") or "")

    for idx, plan in enumerate(plans, start=1):
        disruptor = graph.evaluate_plan_disruptions(plan, now_utc, horizon_days=10)
        capacity_checks = [
            graph.get_lane_capacity(
                lane_id,
                preferred_carrier=(scenario_preferred_carrier or plan.carrier_ids[min(i, len(plan.carrier_ids) - 1)]),
            )
            for i, lane_id in enumerate(plan.lane_ids)
        ]
        min_slots = min((item["available_slots"] for item in capacity_checks), default=0.0)
        reliability = min((item["reliability_score"] for item in capacity_checks), default=0.0)

        blocked_reasons: List[str] = []
        feasibility = True

        if min_slots <= 0:
            feasibility = False
            blocked_reasons.append("No slot capacity on one or more route legs")

        if disruptor.get("hard_block", False):
            feasibility = False
            blocked_reasons.append("Route blocked by active macro disruptor")

        for event in disruptor.get("events", [])[:3]:
            blocked_reasons.append(f"{event.get('event_id', 'EVENT')}: {event.get('reason', 'Disruptor impact')}")

        if customs.get("sanctions_screening_flag", "FALSE").upper() == "TRUE":
            feasibility = False
            blocked_reasons.append("Sanctions screening block active")

        if customs.get("clearance_status", "PENDING").upper() != "CLEARED":
            blocked_reasons.append("Customs is not yet cleared; dispatch requires hold gate")

        eta_delta = (
            (-(plan.transit_hours * 0.08) + (delivery_delay / 60.0 * 0.1))
            + scenario_delay
            + scenario_wait
            + float(disruptor.get("eta_penalty_hours", 0.0) or 0.0)
        )
        cost_delta = (
            (plan.base_cost_usd * 0.05)
            + (scenario_wait * 22.0)
            + float(disruptor.get("cost_penalty_usd", 0.0) or 0.0)
        )

        ripple = estimate_ripple_effect(
            ctx,
            eta_delta_hours=eta_delta,
            action_type="REROUTE_PORT",
            disruptor_event_count=len(disruptor.get("events", [])),
        )

        if scenario_preferred_carrier and scenario_preferred_carrier not in plan.carrier_ids:
            blocked_reasons.append(f"Preferred carrier {scenario_preferred_carrier} not present on this route")

        candidates.append(
            CandidateAction(
                action_id=f"ACT-REROUTE-{idx}",
                action_type="REROUTE_PORT",
                summary=(
                    f"Reroute via {' -> '.join(plan.path_ports)} "
                    f"[{plan.mode_breakdown}] using carrier set {','.join(plan.carrier_ids)}"
                ),
                from_port=plan.path_ports[0],
                to_port=plan.path_ports[-1],
                carrier_id=plan.carrier_ids[0] if plan.carrier_ids else "UNKNOWN",
                lane_ids=plan.lane_ids,
                leg_ids=plan.leg_ids,
                eta_delta_hours=round(eta_delta, 2),
                cost_delta_usd=round(cost_delta, 2),
                reliability_score=round(reliability, 3),
                feasibility=feasibility,
                blocked_reasons=blocked_reasons,
                owner="Transport Planner",
                due_by=due_by,
                evidence={
                    "plan_transit_hours": plan.transit_hours,
                    "plan_variability_hours": plan.variability_hours,
                    "mode_breakdown": plan.mode_breakdown,
                    "modes": plan.modes,
                    "base_sla_probability_pct": base_sla_prob,
                    "lane_min_slots": min_slots,
                    "disruptor": disruptor,
                    "ripple_impact": ripple,
                    "scenario_applied": bool(scenario),
                    "scenario_delay_hours": scenario_delay,
                    "scenario_wait_hours": scenario_wait,
                },
            )
        )

    terminal_id = tos.get("terminal_id", "")
    earliest_slot = graph.get_earliest_terminal_slot(terminal_id) if terminal_id else None
    if earliest_slot:
        queue_depth = float(earliest_slot.get("gate_queue_depth", "0") or 0)
        candidates.append(
            CandidateAction(
                action_id="ACT-SLOT-1",
                action_type="EXPEDITE_TERMINAL_SLOT",
                summary=f"Move shipment to earliest available terminal slot at {terminal_id}",
                from_port=current_port,
                to_port=current_port,
                carrier_id=tms.get("carrier_id", "UNKNOWN"),
                lane_ids=[],
                leg_ids=[],
                eta_delta_hours=round(-0.5 - queue_depth / 180.0, 2),
                cost_delta_usd=150.0,
                reliability_score=0.95,
                feasibility=True,
                blocked_reasons=[],
                owner="Terminal Planner",
                due_by=due_by,
                evidence={
                    "slot_time_utc": earliest_slot.get("slot_time_utc"),
                    "available_slots": earliest_slot.get("available_slots"),
                    "gate_queue_depth": earliest_slot.get("gate_queue_depth"),
                },
            )
        )

    if customs.get("clearance_status", "PENDING").upper() != "CLEARED":
        candidates.append(
            CandidateAction(
                action_id="ACT-CUSTOMS-HOLD-1",
                action_type="CUSTOMS_RESOLUTION",
                summary="Hold dispatch and expedite customs resolution packet",
                from_port=current_port,
                to_port=current_port,
                carrier_id=tms.get("carrier_id", "UNKNOWN"),
                lane_ids=[],
                leg_ids=[],
                eta_delta_hours=1.5,
                cost_delta_usd=50.0,
                reliability_score=0.99,
                feasibility=True,
                blocked_reasons=[],
                owner="Customs and Compliance Officer",
                due_by=due_by,
                evidence={
                    "clearance_status": customs.get("clearance_status", "UNKNOWN"),
                    "document_completeness_pct": customs.get("document_completeness_pct", "0"),
                },
            )
        )

    if tms.get("vehicle_breakdown_flag", "FALSE").upper() == "TRUE":
        lane_hint = plans[0].lane_ids[0] if plans and plans[0].lane_ids else "LANE-APAC-IN-1"
        alt = graph.get_lane_capacity(lane_hint)
        candidates.append(
            CandidateAction(
                action_id="ACT-SWITCH-CARRIER-1",
                action_type="SWITCH_CARRIER",
                summary=f"Switch to alternate carrier {alt['carrier_id']} on lane {lane_hint}",
                from_port=current_port,
                to_port=destination_port,
                carrier_id=str(alt["carrier_id"]),
                lane_ids=[lane_hint],
                leg_ids=[],
                eta_delta_hours=-1.0,
                cost_delta_usd=220.0,
                reliability_score=float(alt["reliability_score"]),
                feasibility=alt["available_slots"] > 0,
                blocked_reasons=[] if alt["available_slots"] > 0 else ["No alternate carrier slot available"],
                owner="Transport Planner",
                due_by=due_by,
                evidence={
                    "alternate_available_slots": alt["available_slots"],
                    "alternate_reliability_score": alt["reliability_score"],
                },
            )
        )

    if not candidates:
        candidates.append(
            CandidateAction(
                action_id="ACT-MONITOR-1",
                action_type="MONITOR_ONLY",
                summary="No feasible operational move found; monitor and re-evaluate in 30 minutes",
                from_port=current_port,
                to_port=destination_port,
                carrier_id=tms.get("carrier_id", "UNKNOWN"),
                lane_ids=[],
                leg_ids=[],
                eta_delta_hours=0.0,
                cost_delta_usd=0.0,
                reliability_score=0.7,
                feasibility=True,
                blocked_reasons=[],
                owner="Control Tower Manager",
                due_by=due_by,
                evidence={"reason": "no_candidates_generated"},
            )
        )

    return candidates
