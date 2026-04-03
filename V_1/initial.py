import csv
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from audit_logger import build_audit_event, write_audit_event
from candidate_engine import generate_candidate_actions
from data_quality import source_health_summary
from data_provider import get_dataset_dir, load_source_rows_with_meta
from decision_scorer import candidate_to_dict, rank_candidates, score_candidate_components
from graph_engine import LogisticsGraph
from post_validator import (
    REQUIRED_OUTPUT_KEYS,
    enforce_hard_constraints,
    enforce_hard_constraints_with_trace,
    make_fallback_output,
    validate_output_shape,
)
from scenario_engine import estimate_scenario_impacts, hybrid_scenario_actionability, parse_scenario_text


ROOT_DIR = Path(__file__).resolve().parent
DATASET_DIR = ROOT_DIR / "datasets"
ENV_FILE = ROOT_DIR / ".env"

GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"

CORE_DATASET_FILES = {
    "tos_terminal": "tos_terminal.csv",
    "tms_transport": "tms_transport.csv",
    "wms_warehouse": "wms_warehouse.csv",
    "customs_compliance": "customs_compliance.csv",
    "erp_finance": "erp_finance.csv",
    "logistics_visibility": "logistics_visibility.csv",
    "iot_telemetry": "iot_telemetry.csv",
}

THRESHOLDS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "tos_terminal.csv": {
        "yard_occupancy_pct": {"op": ">", "val": 85.0, "severity": "CRITICAL", "msg": "Yard occupancy exceeded 85%. Risk of gridlock."},
        "yard_occupancy_rate_of_change": {"op": ">", "val": 5.0, "severity": "HIGH", "msg": "Yard filling rapidly (>5%/hr). Predicts imminent congestion."},
        "container_dwell_time_hrs": {"op": ">", "val": 48.0, "severity": "CRITICAL", "msg": "Container dwell exceeded 48h. Demurrage accruing."},
        "crane_plan_execution_pct": {"op": "<", "val": 75.0, "severity": "MEDIUM", "msg": "Crane execution below 75%. Vessel turnaround delayed."},
        "crane_downtime_min": {"op": ">", "val": 30.0, "severity": "CRITICAL", "msg": "Crane unplanned downtime exceeded 30m."},
        "gate_throughput_trucks_per_hr": {"op": "<", "val": 70.0, "severity": "HIGH", "msg": "Gate bottleneck forming. Throughput degraded."},
        "vessel_departure_delay_min": {"op": ">", "val": 60.0, "severity": "HIGH", "msg": "Vessel delayed by over 1 hour."},
        "cargo_hold_flag": {"op": "==", "val": "TRUE", "severity": "HIGH", "msg": "Cargo hold active at terminal."},
    },
    "tms_transport.csv": {
        "delivery_delay_min": {"op": ">", "val": 30.0, "severity": "HIGH", "msg": "Delivery delay exceeded 30m. SLA risk rising."},
        "carrier_reliability_score": {"op": "<", "val": 0.70, "severity": "MEDIUM", "msg": "Carrier reliability dropped below 70%."},
        "vehicle_breakdown_flag": {"op": "==", "val": "TRUE", "severity": "CRITICAL", "msg": "Vehicle breakdown reported. Rerouting required."},
        "gate_queue_depth": {"op": ">", "val": 30.0, "severity": "HIGH", "msg": "Truck gate queue above 30."},
        "unassigned_shipments_count": {"op": ">", "val": 10.0, "severity": "CRITICAL", "msg": "High unassigned shipment volume."},
        "truck_slot_fill_rate_pct": {"op": "<", "val": 40.0, "severity": "MEDIUM", "msg": "Truck slot fill rate inefficient."},
        "driver_hours_of_service_remaining": {"op": "<", "val": 2.0, "severity": "CRITICAL", "msg": "Driver HOS limit approaching."},
        "spot_market_rate_spike_pct": {"op": ">", "val": 30.0, "severity": "HIGH", "msg": "Spot market rate spike over 30%."},
    },
    "wms_warehouse.csv": {
        "picking_backlog_hours": {"op": ">", "val": 4.0, "severity": "HIGH", "msg": "Picking backlog exceeds 4 hours."},
        "dock_slot_availability": {"op": "==", "val": 0.0, "severity": "CRITICAL", "msg": "No dock slots available."},
        "shift_throughput_units_per_hr": {"op": "<", "val": 100.0, "severity": "MEDIUM", "msg": "Shift throughput below baseline."},
        "dispatch_slot_missed_flag": {"op": "==", "val": "TRUE", "severity": "HIGH", "msg": "Dispatch slot missed."},
        "inventory_readiness_pct": {"op": "<", "val": 80.0, "severity": "HIGH", "msg": "Inventory readiness below 80%."},
    },
    "customs_compliance.csv": {
        "clearance_duration_hrs": {"op": ">", "val": 24.0, "severity": "HIGH", "msg": "Customs clearance pending over 24 hours."},
        "document_completeness_pct": {"op": "<", "val": 100.0, "severity": "CRITICAL", "msg": "Customs documents incomplete."},
        "inspection_flag": {"op": "==", "val": "TRUE", "severity": "HIGH", "msg": "Shipment selected for customs inspection."},
        "holiday_proximity_flag": {"op": "==", "val": "TRUE", "severity": "MEDIUM", "msg": "Holiday closure proximity risk."},
        "sanctions_screening_flag": {"op": "==", "val": "TRUE", "severity": "CRITICAL", "msg": "Sanctions screening hit."},
        "hs_code_mismatch_flag": {"op": "==", "val": "TRUE", "severity": "CRITICAL", "msg": "HS code mismatch detected."},
    },
    "erp_finance.csv": {
        "free_time_expiry_hrs_remaining": {"op": "<", "val": 12.0, "severity": "CRITICAL", "msg": "Free time expires in less than 12 hours."},
        "demurrage_accrual_usd": {"op": ">", "val": 500.0, "severity": "HIGH", "msg": "Demurrage accrual exceeded $500."},
        "sla_breach_probability_pct": {"op": ">", "val": 60.0, "severity": "HIGH", "msg": "SLA breach probability above 60%."},
        "time_to_sla_breach_hrs": {"op": "<", "val": 4.0, "severity": "CRITICAL", "msg": "SLA breach imminent in less than 4 hours."},
    },
    "logistics_visibility.csv": {
        "transhipment_missed_flag": {"op": "==", "val": "TRUE", "severity": "CRITICAL", "msg": "Transshipment connection missed."},
        "load_completion_pct": {"op": "<", "val": 80.0, "severity": "HIGH", "msg": "Load completion below 80%."},
    },
    "iot_telemetry.csv": {
        "temperature_exceedance_duration_min": {"op": ">", "val": 30.0, "severity": "CRITICAL", "msg": "Temperature exceedance over 30 minutes."},
    },
}

SEVERITY_SCORE = {
    "CRITICAL": 100.0,
    "HIGH": 70.0,
    "MEDIUM": 40.0,
}


@dataclass
class ShipmentContext:
    shipment_id: str
    payload: Dict[str, Dict[str, str]]
    alerts: List[Dict[str, str]]
    risk_score: float
    urgency_score: float
    impact_score: float
    feasibility_score: float
    reasons: List[str]


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def to_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_ratio(value: float, high: float) -> float:
    if high <= 0:
        return 0.0
    return max(0.0, min(value / high, 1.0))


def evaluate_condition(actual_value: str, op: str, threshold_value: Any) -> bool:
    if actual_value is None or actual_value.strip() in {"", "-"}:
        return False

    if op == "==":
        return actual_value.strip().upper() == str(threshold_value).upper()

    try:
        actual = float(actual_value)
        threshold = float(threshold_value)
    except ValueError:
        return False

    if op == ">":
        return actual > threshold
    if op == "<":
        return actual < threshold
    if op == ">=":
        return actual >= threshold
    if op == "<=":
        return actual <= threshold
    return False


def parse_response_json(content: str) -> Dict[str, Any]:
    cleaned = content.strip()
    if cleaned.startswith("```"):
        parts = cleaned.split("```")
        if len(parts) >= 3:
            cleaned = parts[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()
    return json.loads(cleaned)


def load_core_datasets(dataset_dir: Path) -> Dict[str, List[Dict[str, str]]]:
    datasets, _ = load_core_datasets_with_meta(dataset_dir)
    return datasets


def load_core_datasets_with_meta(dataset_dir: Path) -> tuple[Dict[str, List[Dict[str, str]]], Dict[str, Dict[str, Any]]]:
    datasets: Dict[str, List[Dict[str, str]]] = {}
    source_meta: Dict[str, Dict[str, Any]] = {}
    strict_quality = os.getenv("STRICT_DATA_QUALITY", "true").strip().lower() in {"1", "true", "yes"}
    for key, filename in CORE_DATASET_FILES.items():
        path = dataset_dir / filename
        rows, meta = load_source_rows_with_meta(key, path)
        health = source_health_summary(key, rows)
        meta["health"] = health
        if strict_quality and health["status"] == "invalid":
            raise ValueError(f"Source validation failed for {key}: {health['errors']}")
        datasets[key] = rows
        source_meta[key] = meta
    return datasets, source_meta


def latest_rows_by_shipment(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    indexed: Dict[str, Dict[str, str]] = {}
    for row in rows:
        sid = row.get("shipment_id", "").strip()
        if sid:
            indexed[sid] = row
    return indexed


def build_indices(datasets: Dict[str, List[Dict[str, str]]]) -> Dict[str, Dict[str, Dict[str, str]]]:
    all_shipments = set()
    indices: Dict[str, Dict[str, Dict[str, str]]] = {}

    for source_name, rows in datasets.items():
        source_index = latest_rows_by_shipment(rows)
        indices[source_name] = source_index
        all_shipments.update(source_index.keys())

    by_shipment: Dict[str, Dict[str, Dict[str, str]]] = {}
    for sid in sorted(all_shipments):
        by_shipment[sid] = {
            "tos_terminal": indices["tos_terminal"].get(sid, {}),
            "tms_transport": indices["tms_transport"].get(sid, {}),
            "wms_warehouse": indices["wms_warehouse"].get(sid, {}),
            "customs_compliance": indices["customs_compliance"].get(sid, {}),
            "erp_finance": indices["erp_finance"].get(sid, {}),
            "logistics_visibility": indices["logistics_visibility"].get(sid, {}),
            "iot_telemetry": indices["iot_telemetry"].get(sid, {}),
        }

    return by_shipment


def evaluate_shipment_alerts(sid: str, payload: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    alerts: List[Dict[str, str]] = []
    for source_name, row in payload.items():
        filename = f"{source_name}.csv"
        rules = THRESHOLDS.get(filename, {})
        for field, rule in rules.items():
            actual_value = row.get(field, "")
            if evaluate_condition(actual_value, str(rule["op"]), rule["val"]):
                alerts.append(
                    {
                        "source": source_name,
                        "shipment_id": sid,
                        "parameter": field,
                        "actual_value": str(actual_value),
                        "threshold_limit": f"{rule['op']} {rule['val']}",
                        "severity": str(rule["severity"]),
                        "message": str(rule["msg"]),
                    }
                )
    return alerts


def score_shipment(sid: str, payload: Dict[str, Dict[str, str]]) -> ShipmentContext:
    alerts = evaluate_shipment_alerts(sid, payload)
    reasons = [item["message"] for item in alerts]

    if alerts:
        urgency = min(100.0, sum(SEVERITY_SCORE.get(a["severity"], 30.0) for a in alerts) / max(len(alerts), 1))
    else:
        urgency = 20.0

    finance = payload.get("erp_finance", {})
    sla_prob = to_float(finance.get("sla_breach_probability_pct", "0"))
    demurrage = to_float(finance.get("demurrage_accrual_usd", "0"))
    free_time_remaining = to_float(finance.get("free_time_expiry_hrs_remaining", "24"))

    impact = 0.55 * sla_prob + 0.30 * (100.0 * safe_ratio(demurrage, 3500.0)) + 0.15 * (100.0 - 100.0 * safe_ratio(free_time_remaining, 72.0))

    customs = payload.get("customs_compliance", {})
    tms = payload.get("tms_transport", {})
    wms = payload.get("wms_warehouse", {})

    feasibility = 100.0
    if customs.get("clearance_status", "").upper() == "REJECTED":
        feasibility -= 30.0
        reasons.append("Customs clearance is rejected")
    if customs.get("sanctions_screening_flag", "FALSE").upper() == "TRUE":
        feasibility -= 50.0
        reasons.append("Sanctions screening block is active")
    if tms.get("vehicle_breakdown_flag", "FALSE").upper() == "TRUE":
        feasibility -= 25.0
        reasons.append("Primary vehicle has broken down")
    if to_float(wms.get("dock_slot_availability", "1")) <= 0:
        feasibility -= 20.0
        reasons.append("No dock slots available")

    feasibility = max(5.0, min(feasibility, 100.0))
    impact = max(0.0, min(impact, 100.0))

    risk = 0.45 * urgency + 0.35 * impact + 0.20 * (100.0 - feasibility)

    return ShipmentContext(
        shipment_id=sid,
        payload=payload,
        alerts=alerts,
        risk_score=round(risk, 2),
        urgency_score=round(urgency, 2),
        impact_score=round(impact, 2),
        feasibility_score=round(feasibility, 2),
        reasons=reasons[:8],
    )


def build_llm_messages(ctx: ShipmentContext, ranked_candidates: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    candidate_catalog = [
        {
            "action_id": item["action_id"],
            "action_type": item["action_type"],
            "summary": item["summary"],
            "from_port": item["from_port"],
            "to_port": item["to_port"],
            "carrier_id": item["carrier_id"],
            "eta_delta_hours": item["eta_delta_hours"],
            "cost_delta_usd": item["cost_delta_usd"],
            "feasibility": item["feasibility"],
            "blocked_reasons": item["blocked_reasons"],
            "score": item["score"],
            "owner": item["owner"],
            "due_by": item["due_by"],
        }
        for item in ranked_candidates[:6]
    ]

    message_payload = {
        "shipment_id": ctx.shipment_id,
        "scores": {
            "risk_score": ctx.risk_score,
            "urgency_score": ctx.urgency_score,
            "impact_score": ctx.impact_score,
            "feasibility_score": ctx.feasibility_score,
        },
        "triggered_alerts": ctx.alerts,
        "signals": ctx.payload,
        "candidate_actions": candidate_catalog,
        "hard_constraints": [
            "Pick selected_action_id ONLY from candidate_actions.action_id.",
            "No dispatch recommendation when customs status is not CLEARED.",
            "No recommendation beyond available operational capacity.",
            "Always include evidence and triggered rules.",
            "Always include owner and due_by.",
        ],
        "output_contract": REQUIRED_OUTPUT_KEYS,
    }

    system_prompt = (
        "You are an operations decision assistant for logistics control tower management. "
        "You must select from provided candidate actions and return strict JSON only."
    )

    user_prompt = (
        "Choose the best action from candidate_actions and provide a ranked list by action_id. "
        "Do not invent new action IDs. Respond as strict JSON with keys exactly matching output_contract.\n\n"
        f"Context:\n{json.dumps(message_payload, indent=2)}"
    )

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def call_groq(messages: List[Dict[str, str]], api_key: str) -> Dict[str, Any]:
    payload = {
        "model": GROQ_MODEL,
        "temperature": 0.1,
        "messages": messages,
    }
    data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        GROQ_ENDPOINT,
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=45) as response:
        response_body = response.read().decode("utf-8")
        parsed = json.loads(response_body)

    content = parsed["choices"][0]["message"]["content"]
    return parse_response_json(content)


def print_exception_board(contexts: List[ShipmentContext]) -> None:
    print("\n=== Exception Board (Risk Ranked) ===")
    print("shipment_id | risk | urgency | impact | feasibility | alerts | critical")
    print("-" * 92)
    for ctx in contexts:
        critical_count = sum(1 for a in ctx.alerts if a["severity"] == "CRITICAL")
        print(
            f"{ctx.shipment_id:10} | {ctx.risk_score:5.1f} | {ctx.urgency_score:7.1f} "
            f"| {ctx.impact_score:6.1f} | {ctx.feasibility_score:11.1f} "
            f"| {len(ctx.alerts):6d} | {critical_count:8d}"
        )


def show_shipment_detail(ctx: ShipmentContext) -> None:
    tos = ctx.payload.get("tos_terminal", {})
    customs = ctx.payload.get("customs_compliance", {})
    finance = ctx.payload.get("erp_finance", {})

    print("\n=== Shipment Detail ===")
    print(f"Shipment: {ctx.shipment_id}")
    print(f"Container: {customs.get('container_id', tos.get('container_id', 'UNKNOWN'))}")
    print(f"Clearance status: {customs.get('clearance_status', 'UNKNOWN')}")
    print(f"Terminal: {tos.get('terminal_id', 'UNKNOWN')}")
    print(f"Risk score: {ctx.risk_score}")
    print(f"Scoring breakdown -> urgency: {ctx.urgency_score}, impact: {ctx.impact_score}, feasibility: {ctx.feasibility_score}")
    print(f"Finance -> SLA breach prob: {finance.get('sla_breach_probability_pct', '0')}%, demurrage: {finance.get('demurrage_accrual_usd', '0')} USD")

    print("Top reasons:")
    if ctx.reasons:
        for item in ctx.reasons[:8]:
            print(f"- {item}")
    else:
        print("- No major risk reason triggered")


def show_candidate_board(ctx: ShipmentContext, ranked_candidates: List[Dict[str, Any]]) -> None:
    print(f"\n=== Candidate Actions for {ctx.shipment_id} ===")
    print("action_id           | type                 | score | eta_delta | cost_delta | feasible | path")
    print("-" * 120)
    for item in ranked_candidates[:8]:
        path = f"{item['from_port']}->{item['to_port']}"
        print(
            f"{item['action_id']:18} | {item['action_type']:20} | {item['score']:5.1f} "
            f"| {item['eta_delta_hours']:9.2f} | {item['cost_delta_usd']:10.2f} | {str(item['feasibility']):8} | {path}"
        )


def run_recommendation(
    ctx: ShipmentContext,
    api_key: str,
    graph: LogisticsGraph,
    now_utc: datetime,
    source_meta: Dict[str, Dict[str, Any]],
) -> None:
    print(f"\n=== Recommendation for {ctx.shipment_id} ===")

    candidates = generate_candidate_actions(ctx, graph, now_utc)
    ranked = rank_candidates(candidates, ctx)
    ranked_candidates = [candidate_to_dict(candidate, score) for candidate, score in ranked]
    candidates_index = {item["action_id"]: item for item in ranked_candidates}

    try:
        if not api_key:
            raise ValueError("Missing GROQ_API_KEY")

        llm_output = call_groq(build_llm_messages(ctx, ranked_candidates), api_key)
        validate_output_shape(llm_output)
        recommendation, constraint_checks = enforce_hard_constraints_with_trace(llm_output, candidates_index, ctx)
        source = "Groq LLM"
    except (ValueError, urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, KeyError):
        recommendation = make_fallback_output(ctx.shipment_id, ranked_candidates, len(ctx.alerts))
        recommendation, constraint_checks = enforce_hard_constraints_with_trace(recommendation, candidates_index, ctx)
        source = "Deterministic fallback"

    selected_action = candidates_index.get(recommendation["selected_action_id"], ranked_candidates[0])
    score_components = {
        candidate.action_id: score_candidate_components(candidate, ctx)
        for candidate, _ in ranked
    }
    merged_source_meta = dict(source_meta)
    merged_source_meta.update({f"graph_{k}": v for k, v in graph.source_metadata.items()})

    audit_event = build_audit_event(
        shipment_id=ctx.shipment_id,
        source_metadata=merged_source_meta,
        selected_action=selected_action,
        ranked_candidates=ranked_candidates,
        score_components=score_components,
        constraint_checks=constraint_checks,
        recommendation=recommendation,
        engine_name="initial.py",
        run_mode=source,
    )
    audit_path = write_audit_event(ROOT_DIR, audit_event)

    print(f"Source: {source}")
    print(f"Audit log: {audit_path}")
    print(f"Selected action: {selected_action['action_id']} | {selected_action['summary']}")
    print(f"Exact plan: {selected_action['from_port']} -> {selected_action['to_port']} via carrier {selected_action['carrier_id']}")
    print(f"Projected impact: eta_delta={selected_action['eta_delta_hours']}h, cost_delta=${selected_action['cost_delta_usd']}")
    print(json.dumps(recommendation, indent=2))


def run_scenario_sandbox(
    ctx: ShipmentContext,
    api_key: str,
    graph: LogisticsGraph,
    now_utc: datetime,
    source_meta: Dict[str, Dict[str, Any]],
) -> None:
    print(f"\n=== Scenario Sandbox for {ctx.shipment_id} ===")
    print("Type scenario text. Example: Delay by 36 hours, wait 2 days, avoid truck in Middle East")
    print("Type 'back' to return to menu.")

    baseline_ranked = rank_candidates(generate_candidate_actions(ctx, graph, now_utc), ctx)
    baseline_candidates = [candidate_to_dict(candidate, score) for candidate, score in baseline_ranked]
    if not baseline_candidates:
        print("No baseline candidates found.")
        return

    baseline_top = baseline_candidates[0]
    print(f"Baseline selected: {baseline_top['action_id']} | {baseline_top['summary']}")
    last_scenario_text = ""
    last_scenario_dict: Dict[str, Any] = {}

    while True:
        scenario_text = input("Scenario> ").strip()
        if not scenario_text:
            print("Please enter a scenario or 'back'.")
            continue
        if scenario_text.lower() in {"back", "exit", "quit"}:
            break

        if scenario_text.lower().startswith("same as previous") and last_scenario_text:
            scenario_text = f"{last_scenario_text}; {scenario_text.replace('same as previous', '').strip()}"

        parsed = parse_scenario_text(scenario_text, api_key, call_groq)
        scenario_dict = parsed.to_dict()
        actionable, reasons, clarification_questions, validation_meta = hybrid_scenario_actionability(
            parsed,
            api_key,
            call_groq,
            min_confidence="MEDIUM",
        )

        if not actionable:
            print("\nScenario is not actionable yet. Please refine the input.")
            for reason in reasons:
                print(f"- {reason}")
            if parsed.unresolved_constraints:
                print("Unresolved constraints:")
                for item in parsed.unresolved_constraints:
                    print(f"- {item}")
            if clarification_questions:
                print("Suggested clarifications:")
                for q in clarification_questions:
                    print(f"- {q}")
            continue

        scenario_ranked = rank_candidates(
            generate_candidate_actions(ctx, graph, datetime.now(timezone.utc), scenario=scenario_dict),
            ctx,
        )
        scenario_candidates = [candidate_to_dict(candidate, score) for candidate, score in scenario_ranked]

        if not scenario_candidates:
            print("No candidates available for this scenario. Try relaxing constraints.")
            continue

        scenario_top = scenario_candidates[0]
        impact = estimate_scenario_impacts(ctx, parsed)

        print("\nScenario parsed:")
        print(json.dumps(scenario_dict, indent=2))
        print("\nBaseline vs Scenario")
        print(f"Baseline action: {baseline_top['action_id']} | eta={baseline_top['eta_delta_hours']}h | cost=${baseline_top['cost_delta_usd']}")
        print(f"Scenario action: {scenario_top['action_id']} | eta={scenario_top['eta_delta_hours']}h | cost=${scenario_top['cost_delta_usd']}")
        print(
            f"SLA delta={impact['sla_probability_delta_pct']}%, "
            f"Demurrage delta=${impact['demurrage_delta_usd']}"
        )

        audit_event = {
            "event_type": "scenario_simulation",
            "event_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "shipment_id": ctx.shipment_id,
            "scenario_text": scenario_text,
            "parsed_scenario": scenario_dict,
            "scenario_validation": validation_meta,
            "baseline_top_action": baseline_top,
            "scenario_top_action": scenario_top,
            "impact_delta": impact,
            "input_sources": {**source_meta, **{f"graph_{k}": v for k, v in graph.source_metadata.items()}},
        }
        audit_path = write_audit_event(ROOT_DIR, audit_event)
        print(f"Scenario audit log: {audit_path}\n")
        last_scenario_text = scenario_text
        last_scenario_dict = scenario_dict


def main() -> None:
    load_env_file(ENV_FILE)
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    dataset_dir = get_dataset_dir(ROOT_DIR)

    if not dataset_dir.exists():
        raise FileNotFoundError(f"Dataset directory not found: {dataset_dir}")

    datasets, source_meta = load_core_datasets_with_meta(dataset_dir)
    shipment_index = build_indices(datasets)
    contexts = [score_shipment(sid, pieces) for sid, pieces in shipment_index.items()]
    contexts.sort(key=lambda c: c.risk_score, reverse=True)

    graph = LogisticsGraph(dataset_dir)

    print("AI Control Tower Terminal Prototype - Graph Decision Engine")
    print("Data sources: TOS, TMS, WMS, Customs, ERP-Finance, Visibility, IoT, Route Graph")

    while True:
        print("\nMenu")
        print("1) Show exception board")
        print("2) Show shipment detail")
        print("3) Show candidate actions for shipment")
        print("4) Generate recommendation for shipment")
        print("5) Generate recommendations for top 3 risks")
        print("6) Scenario Sandbox (chat-like)")
        print("7) Exit")

        choice = input("Select option: ").strip()

        if choice == "1":
            print_exception_board(contexts)
        elif choice == "2":
            sid = input("Enter shipment_id: ").strip()
            ctx = next((c for c in contexts if c.shipment_id == sid), None)
            if not ctx:
                print("Shipment not found.")
                continue
            show_shipment_detail(ctx)
        elif choice == "3":
            sid = input("Enter shipment_id: ").strip()
            ctx = next((c for c in contexts if c.shipment_id == sid), None)
            if not ctx:
                print("Shipment not found.")
                continue
            ranked = rank_candidates(generate_candidate_actions(ctx, graph, datetime.now(timezone.utc)), ctx)
            ranked_candidates = [candidate_to_dict(candidate, score) for candidate, score in ranked]
            show_candidate_board(ctx, ranked_candidates)
        elif choice == "4":
            sid = input("Enter shipment_id: ").strip()
            ctx = next((c for c in contexts if c.shipment_id == sid), None)
            if not ctx:
                print("Shipment not found.")
                continue
            run_recommendation(ctx, api_key, graph, datetime.now(timezone.utc), source_meta)
        elif choice == "5":
            for ctx in contexts[:3]:
                run_recommendation(ctx, api_key, graph, datetime.now(timezone.utc), source_meta)
        elif choice == "6":
            sid = input("Enter shipment_id: ").strip()
            ctx = next((c for c in contexts if c.shipment_id == sid), None)
            if not ctx:
                print("Shipment not found.")
                continue
            run_scenario_sandbox(ctx, api_key, graph, datetime.now(timezone.utc), source_meta)
        elif choice == "7":
            print("Exiting prototype.")
            break
        else:
            print("Invalid option. Try again.")


if __name__ == "__main__":
    main()
