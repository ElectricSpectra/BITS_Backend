import argparse
from datetime import datetime, timezone

from audit_logger import build_audit_event, write_audit_event
from candidate_engine import generate_candidate_actions
from data_provider import get_dataset_dir
from decision_scorer import candidate_to_dict, rank_candidates, score_candidate_components
from graph_engine import LogisticsGraph
from initial import ENV_FILE, ROOT_DIR, build_indices, call_groq, load_core_datasets_with_meta, load_env_file, score_shipment
from post_validator import enforce_hard_constraints_with_trace, make_fallback_output
from scenario_engine import estimate_scenario_impacts, hybrid_scenario_actionability, parse_scenario_text


def _find_context(shipment_id: str, dataset_dir):
    datasets, source_meta = load_core_datasets_with_meta(dataset_dir)
    shipment_index = build_indices(datasets)
    if shipment_id not in shipment_index:
        return None, source_meta
    return score_shipment(shipment_id, shipment_index[shipment_id]), source_meta


def run_for_shipment(shipment_id: str, dataset_dir, scenario_text: str = "", api_key: str = "") -> int:
    ctx, source_meta = _find_context(shipment_id, dataset_dir)
    if ctx is None:
        print(f"Shipment not found: {shipment_id}")
        return 1

    graph = LogisticsGraph(dataset_dir)
    parsed_scenario = None
    scenario_dict = None
    scenario_validation_meta = {}
    if scenario_text.strip():
        parsed_scenario = parse_scenario_text(scenario_text, api_key, call_groq)
        actionable, reasons, clarification_questions, scenario_validation_meta = hybrid_scenario_actionability(
            parsed_scenario,
            api_key,
            call_groq,
            min_confidence="MEDIUM",
        )
        if not actionable:
            print("Scenario is not actionable. Please refine input:")
            for reason in reasons:
                print(f"- {reason}")
            if parsed_scenario.unresolved_constraints:
                print("Unresolved constraints:")
                for item in parsed_scenario.unresolved_constraints:
                    print(f"- {item}")
            if clarification_questions:
                print("Suggested clarifications:")
                for q in clarification_questions:
                    print(f"- {q}")
            return 2
        scenario_dict = parsed_scenario.to_dict()

    ranked = rank_candidates(
        generate_candidate_actions(ctx, graph, datetime.now(timezone.utc), scenario=scenario_dict),
        ctx,
    )
    ranked_candidates = [candidate_to_dict(candidate, score) for candidate, score in ranked]
    candidates_index = {item["action_id"]: item for item in ranked_candidates}

    recommendation = make_fallback_output(ctx.shipment_id, ranked_candidates, len(ctx.alerts))
    recommendation, constraint_checks = enforce_hard_constraints_with_trace(recommendation, candidates_index, ctx)
    selected_action = candidates_index.get(recommendation["selected_action_id"], ranked_candidates[0])
    score_components = {
        candidate.action_id: score_candidate_components(candidate, ctx)
        for candidate, _ in ranked
    }

    print(f"\nProduction Route Decision for {ctx.shipment_id}")
    print("=" * 88)
    print("action_id           | type                 | score | eta_delta | cost_delta | feasible")
    print("-" * 88)
    for item in ranked_candidates[:8]:
        print(
            f"{item['action_id']:18} | {item['action_type']:20} | {item['score']:5.1f} "
            f"| {item['eta_delta_hours']:9.2f} | {item['cost_delta_usd']:10.2f} | {str(item['feasibility']):8}"
        )

    top = selected_action if ranked_candidates else None
    if top:
        print("\nSelected Top Action")
        print("-" * 88)
        print(f"action_id: {top['action_id']}")
        print(f"summary: {top['summary']}")
        print(f"path: {top['from_port']} -> {top['to_port']}")
        print(f"carrier: {top['carrier_id']}")
        if top.get("blocked_reasons"):
            print(f"blocked_reasons: {', '.join(top['blocked_reasons'])}")

    if parsed_scenario:
        impact = estimate_scenario_impacts(ctx, parsed_scenario)
        print("\nScenario applied:")
        print(f"text: {scenario_text}")
        print(f"parsed: {scenario_dict}")
        print(
            f"sla_delta_pct: {impact['sla_probability_delta_pct']}, "
            f"demurrage_delta_usd: {impact['demurrage_delta_usd']}"
        )

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
        engine_name="production_runner.py",
        run_mode="Deterministic fallback",
    )
    if parsed_scenario:
        audit_event["scenario"] = {
            "text": scenario_text,
            "parsed": scenario_dict,
            "validation": scenario_validation_meta,
            "impact": estimate_scenario_impacts(ctx, parsed_scenario),
        }
    audit_path = write_audit_event(ROOT_DIR, audit_event)
    print(f"audit_log: {audit_path}")

    return 0


def main() -> None:
    load_env_file(ENV_FILE)
    dataset_dir = get_dataset_dir(ROOT_DIR)

    parser = argparse.ArgumentParser(
        description="Production runner for shipment route decisions using operational datasets."
    )
    parser.add_argument("shipment_id", help="Shipment id (example: SHP-008)")
    parser.add_argument("--scenario-text", default="", help="Optional free-text what-if scenario")
    parser.add_argument("--api-key", default="", help="Optional API key for scenario parsing")
    args = parser.parse_args()

    raise SystemExit(
        run_for_shipment(
            args.shipment_id.strip(),
            dataset_dir,
            scenario_text=args.scenario_text,
            api_key=args.api_key,
        )
    )


if __name__ == "__main__":
    main()
