import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List


@dataclass
class ScenarioSpec:
    raw_text: str
    delay_hours: float = 0.0
    wait_hours: float = 0.0
    preferred_carrier: str = ""
    blocked_modes: List[str] = field(default_factory=list)
    blocked_regions: List[str] = field(default_factory=list)
    route_preference: str = "FASTEST"
    confidence: str = "LOW"
    unresolved_constraints: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["blocked_modes"] = [m.upper() for m in self.blocked_modes]
        payload["blocked_regions"] = [r.upper() for r in self.blocked_regions]
        payload["route_preference"] = self.route_preference.upper()
        payload["confidence"] = self.confidence.upper()
        return payload


def build_scenario_parse_messages(text: str) -> List[Dict[str, str]]:
    contract = {
        "delay_hours": "float",
        "wait_hours": "float",
        "preferred_carrier": "string",
        "blocked_modes": ["OCEAN", "RAIL", "TRUCK", "AIR", "COASTAL"],
        "blocked_regions": ["APAC", "EU", "NA", "MEA", "LATAM", "AFRICA"],
        "route_preference": "FASTEST|CHEAPEST|LOWEST_RISK|AVOID_TRANSSHIPMENT",
        "confidence": "HIGH|MEDIUM|LOW",
        "unresolved_constraints": ["string"],
    }

    system_prompt = (
        "You are a logistics scenario parser. Convert free-text scenario intent into strict JSON only. "
        "Do not add prose. Use the provided contract keys exactly."
    )
    user_prompt = (
        "Parse the scenario text into strict JSON with this contract. "
        "If any detail is ambiguous, keep the field default and add a note in unresolved_constraints.\n\n"
        f"Contract: {json.dumps(contract)}\n\n"
        f"Scenario text: {text}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def build_scenario_validate_messages(raw_text: str, scenario_payload: Dict[str, Any]) -> List[Dict[str, str]]:
    contract = {
        "actionable": "boolean",
        "confidence": "HIGH|MEDIUM|LOW",
        "missing_fields": ["string"],
        "contradictions": ["string"],
        "clarification_questions": ["string"],
        "reason": "string",
    }

    system_prompt = (
        "You are a strict scenario quality validator for logistics what-if simulation. "
        "Evaluate if the parsed scenario is actionable and safe to execute. Return strict JSON only."
    )
    user_prompt = (
        "Validate this scenario and return JSON with the contract. "
        "If ambiguous, return actionable=false and include clarification questions.\n\n"
        f"Contract: {json.dumps(contract)}\n\n"
        f"Raw scenario text: {raw_text}\n\n"
        f"Parsed scenario JSON: {json.dumps(scenario_payload)}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def _regex_parse_fallback(text: str) -> ScenarioSpec:
    lowered = text.lower()
    spec = ScenarioSpec(raw_text=text, confidence="LOW")

    delay = re.search(r"delay(?:ed)?\s*(?:by)?\s*(\d+(?:\.\d+)?)\s*(hour|hours|hr|hrs|day|days)", lowered)
    if delay:
        value = float(delay.group(1))
        unit = delay.group(2)
        spec.delay_hours = value * 24.0 if unit.startswith("day") else value

    wait = re.search(r"wait\s*(\d+(?:\.\d+)?)\s*(hour|hours|hr|hrs|day|days)", lowered)
    if wait:
        value = float(wait.group(1))
        unit = wait.group(2)
        spec.wait_hours = value * 24.0 if unit.startswith("day") else value

    carrier = re.search(r"carrier\s*(?:to|=|as)?\s*([a-z0-9\-]+)", lowered)
    if carrier:
        spec.preferred_carrier = carrier.group(1).upper()

    mode_map = {
        "ocean": "OCEAN",
        "rail": "RAIL",
        "truck": "TRUCK",
        "road": "TRUCK",
        "air": "AIR",
        "coastal": "COASTAL",
    }
    blocked_modes: List[str] = []
    for token, mode in mode_map.items():
        if f"no {token}" in lowered or f"block {token}" in lowered or f"avoid {token}" in lowered:
            blocked_modes.append(mode)
    spec.blocked_modes = sorted(set(blocked_modes))

    region_map = {
        "middle east": "MEA",
        "apac": "APAC",
        "asia": "APAC",
        "europe": "EU",
        "eu": "EU",
        "north america": "NA",
        "na": "NA",
    }
    blocked_regions: List[str] = []
    for token, region in region_map.items():
        if token in lowered and ("no route" in lowered or "block" in lowered or "avoid" in lowered or "war" in lowered):
            blocked_regions.append(region)
    spec.blocked_regions = sorted(set(blocked_regions))

    if "cheapest" in lowered:
        spec.route_preference = "CHEAPEST"
    elif "lowest risk" in lowered or "safest" in lowered:
        spec.route_preference = "LOWEST_RISK"
    elif "avoid transshipment" in lowered or "no transshipment" in lowered:
        spec.route_preference = "AVOID_TRANSSHIPMENT"
    else:
        spec.route_preference = "FASTEST"

    if spec.delay_hours or spec.wait_hours or spec.blocked_modes or spec.blocked_regions or spec.preferred_carrier:
        spec.confidence = "MEDIUM"
    else:
        spec.unresolved_constraints.append("Could not confidently parse scenario; defaults applied")

    return spec


def parse_scenario_text(
    text: str,
    api_key: str,
    llm_call: Callable[[List[Dict[str, str]], str], Dict[str, Any]],
) -> ScenarioSpec:
    stripped = text.strip()
    if not stripped:
        return ScenarioSpec(raw_text=text, unresolved_constraints=["Empty scenario text"], confidence="LOW")

    if api_key:
        try:
            parsed = llm_call(build_scenario_parse_messages(stripped), api_key)
            spec = ScenarioSpec(
                raw_text=stripped,
                delay_hours=float(parsed.get("delay_hours", 0) or 0),
                wait_hours=float(parsed.get("wait_hours", 0) or 0),
                preferred_carrier=str(parsed.get("preferred_carrier", "") or "").upper(),
                blocked_modes=[str(x).upper() for x in parsed.get("blocked_modes", []) if str(x).strip()],
                blocked_regions=[str(x).upper() for x in parsed.get("blocked_regions", []) if str(x).strip()],
                route_preference=str(parsed.get("route_preference", "FASTEST") or "FASTEST").upper(),
                confidence=str(parsed.get("confidence", "MEDIUM") or "MEDIUM").upper(),
                unresolved_constraints=[str(x) for x in parsed.get("unresolved_constraints", [])],
            )
            return spec
        except Exception:
            return _regex_parse_fallback(stripped)

    return _regex_parse_fallback(stripped)


VALID_MODES = {"OCEAN", "RAIL", "TRUCK", "AIR", "COASTAL"}
VALID_REGIONS = {"APAC", "EU", "NA", "MEA", "LATAM", "AFRICA"}
VALID_PREFERENCES = {"FASTEST", "CHEAPEST", "LOWEST_RISK", "AVOID_TRANSSHIPMENT"}
VALID_CONFIDENCE = {"HIGH", "MEDIUM", "LOW"}


def validate_scenario_spec(spec: ScenarioSpec) -> List[str]:
    errors: List[str] = []

    if spec.delay_hours < 0:
        errors.append("delay_hours cannot be negative")
    if spec.wait_hours < 0:
        errors.append("wait_hours cannot be negative")

    bad_modes = [m for m in (x.upper() for x in spec.blocked_modes) if m not in VALID_MODES]
    if bad_modes:
        errors.append(f"Invalid blocked_modes: {bad_modes}")

    bad_regions = [r for r in (x.upper() for x in spec.blocked_regions) if r not in VALID_REGIONS]
    if bad_regions:
        errors.append(f"Invalid blocked_regions: {bad_regions}")

    if spec.route_preference.upper() not in VALID_PREFERENCES:
        errors.append(f"Invalid route_preference: {spec.route_preference}")

    if spec.confidence.upper() not in VALID_CONFIDENCE:
        errors.append(f"Invalid confidence: {spec.confidence}")

    return errors


def scenario_is_actionable(spec: ScenarioSpec, min_confidence: str = "MEDIUM") -> tuple[bool, List[str]]:
    confidence_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
    reasons: List[str] = []

    schema_errors = validate_scenario_spec(spec)
    if schema_errors:
        reasons.extend(schema_errors)

    required_rank = confidence_rank.get(min_confidence.upper(), 2)
    current_rank = confidence_rank.get(spec.confidence.upper(), 1)
    if current_rank < required_rank:
        reasons.append(f"Scenario confidence {spec.confidence.upper()} is below required {min_confidence.upper()}")

    if spec.unresolved_constraints and spec.confidence.upper() == "LOW":
        reasons.append("Scenario contains unresolved constraints with low confidence")

    return (len(reasons) == 0), reasons


def validate_scenario_with_llm(
    spec: ScenarioSpec,
    api_key: str,
    llm_call: Callable[[List[Dict[str, str]], str], Dict[str, Any]],
) -> Dict[str, Any]:
    default = {
        "actionable": True,
        "confidence": spec.confidence.upper(),
        "missing_fields": [],
        "contradictions": [],
        "clarification_questions": [],
        "reason": "LLM validation skipped",
    }

    if not api_key:
        return default

    try:
        response = llm_call(build_scenario_validate_messages(spec.raw_text, spec.to_dict()), api_key)
        actionable = bool(response.get("actionable", True))
        confidence = str(response.get("confidence", spec.confidence) or spec.confidence).upper()
        if confidence not in VALID_CONFIDENCE:
            confidence = spec.confidence.upper()

        return {
            "actionable": actionable,
            "confidence": confidence,
            "missing_fields": [str(x) for x in response.get("missing_fields", [])],
            "contradictions": [str(x) for x in response.get("contradictions", [])],
            "clarification_questions": [str(x) for x in response.get("clarification_questions", [])],
            "reason": str(response.get("reason", "LLM semantic validation complete")),
        }
    except Exception:
        return {
            "actionable": True,
            "confidence": spec.confidence.upper(),
            "missing_fields": [],
            "contradictions": [],
            "clarification_questions": [],
            "reason": "LLM validation failed; fallback to deterministic validation",
        }


def hybrid_scenario_actionability(
    spec: ScenarioSpec,
    api_key: str,
    llm_call: Callable[[List[Dict[str, str]], str], Dict[str, Any]],
    min_confidence: str = "MEDIUM",
) -> tuple[bool, List[str], List[str], Dict[str, Any]]:
    deterministic_ok, deterministic_reasons = scenario_is_actionable(spec, min_confidence=min_confidence)
    llm_validation = validate_scenario_with_llm(spec, api_key, llm_call)

    merged_reasons = list(deterministic_reasons)
    merged_questions: List[str] = list(llm_validation.get("clarification_questions", []))

    missing_fields = llm_validation.get("missing_fields", [])
    contradictions = llm_validation.get("contradictions", [])
    if missing_fields:
        merged_reasons.append(f"LLM flagged missing fields: {missing_fields}")
    if contradictions:
        merged_reasons.append(f"LLM flagged contradictions: {contradictions}")

    confidence_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
    llm_conf = str(llm_validation.get("confidence", spec.confidence)).upper()
    required_rank = confidence_rank.get(min_confidence.upper(), 2)
    llm_rank = confidence_rank.get(llm_conf, 1)
    llm_conf_ok = llm_rank >= required_rank

    if not llm_conf_ok:
        merged_reasons.append(f"LLM confidence {llm_conf} is below required {min_confidence.upper()}")

    llm_actionable = bool(llm_validation.get("actionable", True))
    if not llm_actionable:
        merged_reasons.append(str(llm_validation.get("reason", "LLM marked scenario as non-actionable")))

    is_actionable = deterministic_ok and llm_actionable and llm_conf_ok and not missing_fields and not contradictions
    validation_meta = {
        "deterministic_ok": deterministic_ok,
        "llm_actionable": llm_actionable,
        "llm_confidence": llm_conf,
        "required_confidence": min_confidence.upper(),
        "llm_reason": llm_validation.get("reason", ""),
        "missing_fields": missing_fields,
        "contradictions": contradictions,
        "clarification_questions": merged_questions,
    }

    return is_actionable, merged_reasons, merged_questions, validation_meta


def estimate_scenario_impacts(ctx: Any, scenario: ScenarioSpec) -> Dict[str, float]:
    finance = ctx.payload.get("erp_finance", {})
    base_sla = float(finance.get("sla_breach_probability_pct", "50") or 50)
    base_demurrage = float(finance.get("demurrage_accrual_usd", "0") or 0)

    delay_pressure = (scenario.delay_hours * 1.2) + (scenario.wait_hours * 1.0)
    scenario_sla = max(0.0, min(100.0, base_sla + delay_pressure))

    demurrage_per_hour = 22.0
    scenario_demurrage = base_demurrage + (scenario.wait_hours * demurrage_per_hour)

    return {
        "base_sla_breach_probability_pct": round(base_sla, 2),
        "scenario_sla_breach_probability_pct": round(scenario_sla, 2),
        "sla_probability_delta_pct": round(scenario_sla - base_sla, 2),
        "base_demurrage_usd": round(base_demurrage, 2),
        "scenario_demurrage_usd": round(scenario_demurrage, 2),
        "demurrage_delta_usd": round(scenario_demurrage - base_demurrage, 2),
    }
