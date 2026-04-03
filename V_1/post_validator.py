from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List


REQUIRED_OUTPUT_KEYS = [
    "issue_summary",
    "selected_action_id",
    "ranked_action_ids",
    "rationale_per_action_id",
    "evidence_used",
    "triggered_rules",
    "expected_impact",
    "owners",
    "due_by",
    "confidence_band",
    "missing_data",
]


def validate_output_shape(rec: Dict[str, Any]) -> None:
    for key in REQUIRED_OUTPUT_KEYS:
        if key not in rec:
            raise ValueError(f"Missing output key: {key}")


def enforce_hard_constraints(rec: Dict[str, Any], candidates_index: Dict[str, Dict[str, Any]], ctx: Any) -> Dict[str, Any]:
    constrained, _ = enforce_hard_constraints_with_trace(rec, candidates_index, ctx)
    return constrained


def enforce_hard_constraints_with_trace(
    rec: Dict[str, Any], candidates_index: Dict[str, Dict[str, Any]], ctx: Any
) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    checks: List[Dict[str, Any]] = []
    customs = ctx.payload.get("customs_compliance", {})
    clearance_status = customs.get("clearance_status", "UNKNOWN").upper()

    selected_id = rec.get("selected_action_id", "")
    ranked_ids = [str(item) for item in rec.get("ranked_action_ids", [])]

    if selected_id not in candidates_index and ranked_ids:
        checks.append(
            {
                "check": "selected_in_candidates",
                "status": "warn",
                "details": "selected_action_id missing; switched to first ranked action",
            }
        )
        selected_id = ranked_ids[0]

    if selected_id not in candidates_index:
        fallback_id = next(iter(candidates_index.keys()))
        rec["selected_action_id"] = fallback_id
        rec["ranked_action_ids"] = [fallback_id]
        rec["triggered_rules"] = list(rec.get("triggered_rules", [])) + ["Selected action id not found; replaced with deterministic top candidate"]
        selected_id = fallback_id
        checks.append(
            {
                "check": "selected_in_candidates",
                "status": "enforced",
                "details": f"selected action replaced with {fallback_id}",
            }
        )
    else:
        checks.append({"check": "selected_in_candidates", "status": "pass", "details": selected_id})

    selected = candidates_index[selected_id]

    if clearance_status != "CLEARED" and selected.get("action_type") in {"REROUTE_PORT", "SWITCH_CARRIER", "EXPEDITE_TERMINAL_SLOT"}:
        customs_candidates = [cid for cid, item in candidates_index.items() if item.get("action_type") == "CUSTOMS_RESOLUTION"]
        if customs_candidates:
            safe_id = customs_candidates[0]
            rec["selected_action_id"] = safe_id
            rec["ranked_action_ids"] = [safe_id] + [cid for cid in ranked_ids if cid != safe_id]
            rec["triggered_rules"] = list(rec.get("triggered_rules", [])) + ["Dispatch-related action replaced due to uncleared customs"]
            checks.append(
                {
                    "check": "customs_dispatch_block",
                    "status": "enforced",
                    "details": f"uncleared customs ({clearance_status}), replaced with {safe_id}",
                }
            )
        else:
            checks.append(
                {
                    "check": "customs_dispatch_block",
                    "status": "warn",
                    "details": "uncleared customs but no CUSTOMS_RESOLUTION candidate available",
                }
            )
    else:
        checks.append(
            {
                "check": "customs_dispatch_block",
                "status": "pass",
                "details": clearance_status,
            }
        )

    if not rec.get("owners"):
        rec["owners"] = ["Control Tower Manager"]
        checks.append({"check": "owners_present", "status": "enforced", "details": "default owner applied"})
    else:
        checks.append({"check": "owners_present", "status": "pass", "details": "owners provided"})

    if not rec.get("due_by"):
        rec["due_by"] = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        checks.append({"check": "due_by_present", "status": "enforced", "details": "default due_by applied"})
    else:
        checks.append({"check": "due_by_present", "status": "pass", "details": rec.get("due_by")})

    return rec, checks


def make_fallback_output(shipment_id: str, ranked_candidates: List[Dict[str, Any]], alert_count: int) -> Dict[str, Any]:
    top = ranked_candidates[0]
    ranked_ids = [item["action_id"] for item in ranked_candidates[:5]]

    return {
        "issue_summary": f"Shipment {shipment_id} is at risk and requires executable intervention",
        "selected_action_id": top["action_id"],
        "ranked_action_ids": ranked_ids,
        "rationale_per_action_id": {
            item["action_id"]: f"Score={item['score']}, feasibility={item['feasibility']}, eta_delta={item['eta_delta_hours']}h, cost_delta=${item['cost_delta_usd']}"
            for item in ranked_candidates[:5]
        },
        "evidence_used": {
            "alert_count": alert_count,
            "candidate_count": len(ranked_candidates),
            "selected_score": top["score"],
        },
        "triggered_rules": [
            "Candidate action generated from graph route and capacity data",
            "Hard constraints validated post selection",
        ],
        "expected_impact": "Reduce SLA breach and operational risk using highest-score feasible candidate",
        "owners": [top.get("owner", "Control Tower Manager")],
        "due_by": top.get("due_by"),
        "confidence_band": "Medium" if alert_count >= 4 else "High",
        "missing_data": [],
    }
