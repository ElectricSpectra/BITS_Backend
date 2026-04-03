# Version 1 - Production Decision Engine (Current State)

## 1) What This App Is

This app is a logistics control-tower decision engine that:

- ingests operational shipment signals from multiple enterprise sources,
- detects risk via rule thresholds,
- generates executable candidate actions,
- ranks them deterministically,
- optionally asks an LLM to choose from those candidates,
- enforces hard safety/operational constraints,
- emits a traceable audit record for every decision.

It is not a generic chatbot recommender. It is a constrained operations engine with a deterministic fallback path.


## 2) Core Runtime Components

### `initial.py` (interactive control tower app)

- Loads source data and metadata.
- Computes risk-ranked exception board.
- Shows shipment-level details.
- Generates recommendation per shipment (LLM or fallback).
- Provides a chat-like scenario sandbox for iterative what-if simulation.
- Writes structured decision audit events.


### `production_runner.py` (single-shipment production entrypoint)

- Non-interactive runner for one shipment ID.
- Always works with deterministic recommendation path currently.
- Performs the same candidate generation/ranking/constraint checks.
- Supports `--scenario-text` for scripted what-if simulation runs.
- Writes the same structured JSONL audit records.


### `scenario_engine.py` (text-to-scenario parser and simulator)

- Converts free-text planner scenarios into structured scenario JSON.
- Uses LLM parsing when available, regex fallback when unavailable.
- Preserves original text and unresolved constraints.
- Computes baseline vs scenario impact deltas (SLA and demurrage).


### `graph_engine.py` (routing/planning graph)

- Loads planning datasets (ports, legs, capacities, slots, policies, commitments).
- Builds fast indices for route and capacity lookup.
- Enumerates candidate route plans using N-hop DFS (`max_hops` configurable).
- Captures planning-source metadata for auditability.


### `candidate_engine.py` (action generation)

- Converts shipment risk context + graph plans into executable candidates.
- Candidate types include:
  - `REROUTE_PORT`
  - `EXPEDITE_TERMINAL_SLOT`
  - `CUSTOMS_RESOLUTION`
  - `SWITCH_CARRIER`
  - `MONITOR_ONLY` fallback
- Adds ETA delta, cost delta, feasibility, blocked reasons, owner and due time.


### `decision_scorer.py` (deterministic prioritization)

- Scores each candidate with explicit components.
- Provides both total score and component breakdown:
  - feasibility
  - ETA
  - cost
  - reliability
  - risk relief


### `post_validator.py` (hard constraints + trace)

- Validates output contract keys.
- Enforces hard constraints (example: customs block for dispatch-like actions).
- Repairs missing owner/due-by.
- Emits constraint-check trace with pass/warn/enforced status.


### `data_provider.py` (source abstraction)

- Loads data from:
  - default local CSV,
  - per-source path override,
  - per-source URL override.
- Emits per-source metadata (version/timestamp/row count/type/ref).


### `audit_logger.py` (observability sink)

- Builds a structured decision audit event.
- Writes JSON lines (`.jsonl`) sink for compliance and replay.


## 3) Data Inputs and Contracts (Current)

### Operational risk inputs

- `tos_terminal.csv`
- `tms_transport.csv`
- `wms_warehouse.csv`
- `customs_compliance.csv`
- `erp_finance.csv`
- `logistics_visibility.csv`
- `iot_telemetry.csv`


### Planning/routing inputs

- `port_nodes.csv`
- `route_legs.csv`
- `vessel_schedules.csv`
- `carrier_capacity.csv`
- `terminal_slots.csv`
- `policy_constraints.csv`
- `customer_commitments.csv`


### Source override model (already implemented)

Per source, runtime can consume from env without code change:

- `SOURCE_<SOURCE_NAME>_PATH`
- `SOURCE_<SOURCE_NAME>_URL`

Global default data directory override:

- `CONTROL_TOWER_DATA_DIR`


## 4) End-to-End Decision Flow

1. Load operational + planning sources and source metadata.
2. Build per-shipment indexed payload.
3. Apply thresholds to produce shipment alerts.
4. Score risk with urgency/impact/feasibility blend.
5. Use graph to enumerate route plans up to `ROUTE_MAX_HOPS`.
6. Generate candidate actions with feasibility and evidence.
7. Deterministically score/rank candidates.
8. If LLM key is available:
	- ask LLM to select only from provided action IDs.
9. If LLM fails/missing:
	- deterministic fallback recommendation.
10. Enforce hard constraints with trace.
11. Emit final recommendation + append audit event to JSONL sink.


## 5) Current Capabilities

### Decision capabilities

- Risk-ranked exception board.
- Shipment-level explainability (scores + reasons).
- Actionable recommendation output (not generic text).
- Candidate ranking with deterministic math.
- Safety constraint enforcement.


### Multi-modal capabilities (current implementation)

- Uses `mode` from `route_legs.csv`.
- Supports route plans with multiple legs and mode breakdown (for example `OCEAN->COASTAL`).
- N-hop route enumeration via DFS.
- `ROUTE_MAX_HOPS` controls exploration depth.
- Scenario-aware route filtering supports blocked modes/regions and route preference.

Note: this is production-integrated multimodal planning within existing route dataset contract, not a separate demo branch.


### Observability/compliance capabilities

Each decision event logs:

- input source references,
- source versions/timestamps,
- row counts,
- selected candidate,
- blocked reasons,
- score components per action,
- ranked action list,
- post-validator constraint checks,
- final recommendation payload.
- scenario text, parsed scenario JSON, and scenario impact deltas.


## 6) Audit Trail Format (What is Recorded)

One JSON object per line in `logs/decision_audit.jsonl` containing:

- `event_type`, `event_time_utc`, `shipment_id`
- `engine`, `run_mode`
- `input_sources` (ops + graph source metadata)
- `selected_candidate`
- `score_components` (per candidate)
- `ranked_candidates`
- `constraint_checks`
- `recommendation`

This supports compliance review, forensic replay, and post-incident analysis.


## 7) Accuracy and Quality Limitations (Current)

### A) Accuracy limitations

- Rule thresholds are static and hand-tuned; no auto-calibration.
- ETA and cost deltas are heuristic approximations.
- Score weights are fixed, not learned from historical outcomes.
- Route feasibility still depends on quality/granularity of lane capacity data.


### B) Implementation limitations

- Interactive mode can exit with input/EOF edge cases if run in non-interactive context.
- No strict schema validator yet (missing/typed fields are not fail-fast blocked).
- No freshness SLA checks yet (stale data can still be consumed).
- No built-in idempotency key for duplicate decision event suppression.
- JSONL sink is local-file based only (no native queue/SIEM sink in core yet).


### C) Data quality limitations

- If upstream rows are stale/incomplete, recommendations degrade.
- Source coverage is shipment-keyed; partial source availability can bias scoring.
- Policy/commitment tables are only as accurate as operational maintenance.
- Inconsistent timestamp quality across systems can reduce temporal reliability.


## 8) What Must Be Improved Next (Priority Roadmap)

### Priority 0: Data trust and correctness

1. Add strict schema validation per source:
	- required fields,
	- field types,
	- allowed enums,
	- null handling policy.
2. Add freshness gates:
	- reject/block stale sources by max-age policy.
3. Add source health summary in decision output and audit event.


### Priority 1: Decision accuracy and robustness

1. Integrate vessel cutoff and slot-time feasibility into route scoring.
2. Add uncertainty bands using variability fields (`P50/P90 ETA`).
3. Move hardcoded weights/thresholds into versioned config.
4. Add scenario/backtest harness using historical outcomes.


### Priority 2: Operability and compliance

1. Add external sink support:
	- webhook,
	- message queue,
	- SIEM/ELK forwarder.
2. Add decision correlation IDs and idempotency keys.
3. Add signed audit envelope (tamper-evident trail).


### Priority 3: Productization

1. Expose API endpoints (decision request/response + health).
2. Add unit tests for each engine module.
3. Add integration tests with source override matrix.
4. Add policy simulation mode (what-if before execution).


## 9) Suggested KPI Set for Version 1 Operations

- Recommendation acceptance rate by planners.
- SLA breach reduction against baseline.
- False-positive alert ratio.
- Mean decision latency per shipment.
- % decisions with complete source coverage.
- % decisions blocked by hard constraints.


## 10) Current Version Maturity Summary

This version is a strong **rule-driven, deterministic decision-assist engine with auditability**, suitable for controlled operational usage.

It is not yet a full autonomous optimizer because:

- data quality gates are not strict enough,
- scoring is heuristic and not outcome-calibrated,
- freshness/schema governance is not fully enforced,
- external enterprise observability sinks are pending.

With schema/freshness gates + calibration + queue-backed audit sinks, this can move from pilot-grade to production-grade reliability.

