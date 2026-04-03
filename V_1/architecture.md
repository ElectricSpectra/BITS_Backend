## Content for architecture.md

# AI Control Tower Orchestration Architecture

## 1. Purpose
Build an AI orchestration layer that connects terminal, transport, warehouse, customs, logistics visibility, and finance signals to recommend next-best-actions for at-risk shipments.

## 2. Primary Users
1. Regional Operations Manager
2. Control Tower Manager

## 3. Secondary Users
1. Terminal Planner
2. Transport Planner
3. Customs and Compliance Officer
4. Account and Service Manager

## 4. Core Outcomes
1. Reduce SLA breaches
2. Reduce demurrage and detention cost
3. Reduce exception resolution time
4. Improve cross-team coordination
5. Provide explainable decision recommendations

## 5. High-Level Architecture Layers
1. Source Systems Layer
2. Ingestion and Integration Layer
3. Normalization and Data Quality Layer
4. Decision Intelligence Layer
5. Action and Workflow Layer
6. Experience Layer (UI and Alerts)
7. Security, Governance, and Observability Layer

## 6. Data Sources and Required Inputs

### 6.1 Terminal Operating System (TOS)
1. Container events: gate-in, gate-out, discharge, load, yard move
2. Yard occupancy and block utilization
3. Berth schedule, vessel ETA and ETD
4. Crane plan and execution status
5. Gate queue and wait time

### 6.2 Transport Management System (TMS)
1. Truck capacity by slot and lane
2. Vehicle GPS and movement status
3. Pickup and delivery planned vs actual timestamps
4. Route alternatives and estimated travel duration
5. Carrier reliability history

### 6.3 Warehouse Management System (WMS)
1. Inventory readiness
2. Picking and packing backlog
3. Dock and dispatch slot availability
4. Cargo hold and release status
5. Throughput by shift

### 6.4 Customs and Compliance Systems
1. Clearance status by shipment and container
2. Document completeness
3. Hold, reject, and inspection reasons
4. Risk indicators
5. Estimated clearance time

### 6.5 Logistics Booking and Visibility Platform
1. Customer commitments and milestones
2. Planned route and promised ETA
3. Booking priority and service type
4. Milestone exceptions
5. Partner network event updates

### 6.6 ERP and Finance
1. SLA penalty terms
2. Demurrage and detention rates
3. Customer tier and shipment value
4. Route and provider cost benchmarks
5. Invoice and billing status

### 6.7 Optional Tracking and IoT
1. Temperature and condition alerts
2. Asset sensor events
3. Yard and gate telemetry

## 7. Integration Patterns
1. API connectors for modern systems
2. Event streaming for near real-time updates
3. EDI adapters for legacy partners
4. Batch file ingestion for periodic exports
5. Retry and dead-letter handling for failed messages

## 8. Canonical Data Model (Normalization)
Use one common model to join all systems:
1. Shipment
2. Container
3. Transport Leg
4. Location
5. Event
6. Exception
7. Recommendation
8. Task and Workflow

Minimum common fields:
1. Global shipment id
2. Container id
3. Source system id
4. Event type and timestamp
5. Location code
6. Status
7. SLA deadline
8. Cost and risk metadata

## 9. Decision Intelligence Layer
1. Rulebook engine for deterministic business and compliance constraints
2. Context assembler that gathers shipment, terminal, transport, customs, and finance facts into one grounded prompt
3. LLM decision engine (Llama via Groq) that generates ranked next actions
4. Deterministic post-validator that checks generated actions against hard constraints
5. Explainability module showing why the recommendation was made

Example recommendation payload:
1. Issue detected
2. Recommended action list (ranked)
3. Why this action (evidence and triggered rules)
4. Expected business impact estimate
5. Required owner
6. Due-by timestamp
7. Confidence band based on data completeness and rule-match quality

## 10. Action and Workflow Layer
1. Alert generation and prioritization
2. Human approval workflow
3. Task creation and assignment
4. Escalation matrix and SLA timers
5. Outcome capture for learning loop

## 11. UI and Experience Layer
1. Exception board
2. Shipment risk detail
3. Action center
4. Simulation panel
5. KPI dashboard
6. Audit trail view

## 12. Security and Governance
1. Role-based access control
2. Data masking for sensitive fields
3. Full audit logs of recommendation and approval actions
4. Data lineage and source traceability
5. Retention policies by data class
6. Compliance-ready logs for investigations

## 13. Observability and Reliability
1. Connector health monitoring
2. Ingestion lag monitoring
3. Data quality scorecards
4. Prompt quality and grounding coverage tracking
5. Recommendation acceptance and override tracking
6. Graceful degradation when one source is unavailable
7. Service-level objectives for latency and uptime

## 14. GenAI Rulebook (Llama on Groq)

### 14.1 Objective
Generate safe, explainable, and executable next-best-actions for at-risk shipments.

### 14.2 Required Input Context
1. Shipment snapshot: status, milestones, SLA deadline, customer tier
2. Terminal state: yard occupancy, gate queue, berth and vessel timing
3. Transport state: truck availability, ETA drift, route alternatives
4. Customs state: clearance status, hold reasons, missing documents
5. Finance state: penalty terms, demurrage exposure, cost benchmarks
6. Operational constraints: cut-off times, resource limits, and non-negotiable policies

### 14.3 Hard Constraints (must never be violated)
1. Do not recommend dispatch if customs hold is active unless hold is explicitly cleared
2. Do not recommend actions beyond available resource capacity
3. Respect contractual and regulatory restrictions by region
4. If critical fields are missing, return insufficient-data status plus required data list
5. Every recommendation must include owner and deadline

### 14.4 Decision Types to Generate
1. Route decision
2. Provider decision
3. Priority decision
4. Resource decision
5. Exception resolution decision

### 14.5 Prioritization Logic (rule-based, non-ML)
1. Urgency score from time-to-SLA and time-to-demurrage
2. Impact score from customer tier, penalty exposure, and shipment value
3. Feasibility score from resource availability and operational constraints
4. Final rank based on weighted composite of urgency, impact, and feasibility

### 14.6 Output Contract (strict JSON)
1. issue_summary
2. ranked_actions
3. rationale_per_action
4. evidence_used
5. triggered_rules
6. expected_impact
7. owners
8. due_by
9. confidence_band
10. missing_data

### 14.7 Confidence Definition
1. High: complete critical data plus strong rule alignment and no policy conflicts
2. Medium: partial non-critical gaps plus acceptable rule alignment
3. Low: missing critical fields or conflicting source data

### 14.8 Safety and Human-in-the-loop
1. High-impact actions require manager approval
2. Compliance-related actions require compliance owner acknowledgment
3. All decisions and overrides must be audit-logged

### 14.9 LLM Policy Prompt Blueprint
Use this policy prompt for the LLM orchestration service:

```text
You are an operations decision assistant for logistics control tower management.
Your task is to generate ranked next-best-actions for shipment exceptions.

You must:
- Follow all hard constraints and compliance rules.
- Use only provided context; do not invent facts.
- Return actions that are executable with owners and deadlines.
- Explain why each action is recommended.
- If data is insufficient, return missing_data and safe fallback actions.

Hard constraints:
1) No dispatch recommendation when customs hold is active unless clearance is confirmed.
2) No recommendation exceeding available capacity.
3) Respect regional policy and contract constraints.
4) Always include evidence and triggered rules.
5) Output must follow strict JSON schema.

Decision priorities:
- Minimize SLA breach risk.
- Minimize demurrage and penalty cost.
- Maximize operational feasibility.
- Preserve customer commitments.
```

## 15. MVP Scope for Hackathon
1. Focus loop: container exception orchestration across TOS, TMS, customs
2. Five decision types: route, provider, priority, resource, exception
3. One unified exception dashboard
4. One approval workflow
5. KPI output: avoided delay, avoided penalty, improved on-time rate

## 16. KPIs to Track
1. Exception detection to action time
2. Exception closure time
3. SLA breach rate
4. Demurrage and detention spend
5. Recommendation acceptance rate
6. Manual coordination effort reduction

---