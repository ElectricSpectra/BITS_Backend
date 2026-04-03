import json
import os
import hashlib
import hmac
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _audit_path(root_dir: Path) -> Path:
    configured = os.getenv("AUDIT_LOG_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return (root_dir / "logs" / "decision_audit.jsonl").resolve()


def build_audit_event(
    shipment_id: str,
    source_metadata: Dict[str, Dict[str, Any]],
    selected_action: Dict[str, Any],
    ranked_candidates: List[Dict[str, Any]],
    score_components: Dict[str, Dict[str, float]],
    constraint_checks: List[Dict[str, Any]],
    recommendation: Dict[str, Any],
    engine_name: str,
    run_mode: str,
) -> Dict[str, Any]:
    event = {
        "event_type": "shipment_decision",
        "event_time_utc": _utc_now_iso(),
        "correlation_id": str(uuid.uuid4()),
        "shipment_id": shipment_id,
        "engine": engine_name,
        "run_mode": run_mode,
        "input_sources": source_metadata,
        "selected_candidate": {
            "action_id": selected_action.get("action_id"),
            "action_type": selected_action.get("action_type"),
            "mode_breakdown": selected_action.get("mode_breakdown", ""),
            "from_port": selected_action.get("from_port"),
            "to_port": selected_action.get("to_port"),
            "carrier_id": selected_action.get("carrier_id"),
            "eta_delta_hours": selected_action.get("eta_delta_hours"),
            "cost_delta_usd": selected_action.get("cost_delta_usd"),
            "blocked_reasons": selected_action.get("blocked_reasons", []),
        },
        "score_components": score_components,
        "ranked_candidates": [
            {
                "action_id": item.get("action_id"),
                "action_type": item.get("action_type"),
                "score": item.get("score"),
                "mode_breakdown": item.get("mode_breakdown", ""),
                "feasible": item.get("feasibility"),
                "blocked_reasons": item.get("blocked_reasons", []),
            }
            for item in ranked_candidates[:8]
        ],
        "constraint_checks": constraint_checks,
        "recommendation": recommendation,
    }

    idempotency_raw = json.dumps(
        {
            "shipment_id": shipment_id,
            "engine": engine_name,
            "selected_action_id": event["selected_candidate"].get("action_id"),
            "due_by": recommendation.get("due_by"),
            "source_versions": {k: v.get("source_version", "") for k, v in source_metadata.items()},
        },
        sort_keys=True,
    )
    event["idempotency_key"] = hashlib.sha256(idempotency_raw.encode("utf-8")).hexdigest()

    signing_key = os.getenv("AUDIT_SIGNING_KEY", "").encode("utf-8")
    payload_for_sig = json.dumps(event, sort_keys=True).encode("utf-8")
    if signing_key:
        event["signature"] = hmac.new(signing_key, payload_for_sig, hashlib.sha256).hexdigest()
        event["signature_type"] = "HMAC-SHA256"
    else:
        event["signature"] = hashlib.sha256(payload_for_sig).hexdigest()
        event["signature_type"] = "SHA256"

    return event


def write_audit_event(root_dir: Path, event: Dict[str, Any]) -> Path:
    path = _audit_path(root_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=True) + "\n")

    webhook = os.getenv("AUDIT_WEBHOOK_URL", "").strip()
    if webhook:
        try:
            data = json.dumps(event).encode("utf-8")
            request = urllib.request.Request(
                webhook,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=10):
                pass
        except Exception:
            # Non-blocking sink failure: local audit write is already persisted.
            pass

    return path
