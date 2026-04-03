from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from data_provider import load_source_rows_with_meta
from disruptor_engine import evaluate_route_disruptors


@dataclass
class RoutePlan:
    path_ports: List[str]
    leg_ids: List[str]
    lane_ids: List[str]
    carrier_ids: List[str]
    transit_hours: float
    variability_hours: float
    base_cost_usd: float
    modes: List[str]
    mode_breakdown: str


class LogisticsGraph:
    def __init__(self, dataset_dir: Path) -> None:
        self.dataset_dir = dataset_dir
        self.source_metadata: Dict[str, Dict[str, Any]] = {}
        self.port_nodes = self._read_csv("port_nodes.csv")
        self.route_legs = self._read_csv("route_legs.csv")
        self.vessel_schedules = self._read_csv("vessel_schedules.csv")
        self.carrier_capacity = self._read_csv("carrier_capacity.csv")
        self.terminal_slots = self._read_csv("terminal_slots.csv")
        self.policy_constraints = self._read_csv("policy_constraints.csv")
        self.customer_commitments = self._read_csv("customer_commitments.csv")
        self.macro_disruptors = self._read_csv("macro_disruptors.csv")

        self.terminal_port_index = {row["terminal_id"]: row["port_code"] for row in self.terminal_slots if row.get("terminal_id")}
        self.customer_commitments_index = {row["shipment_id"]: row for row in self.customer_commitments if row.get("shipment_id")}
        self.policy_index = {row["region"]: row for row in self.policy_constraints if row.get("region")}

        self.lane_capacity_index: Dict[str, List[Dict[str, str]]] = {}
        for row in self.carrier_capacity:
            lane_id = row.get("lane_id", "")
            if lane_id:
                self.lane_capacity_index.setdefault(lane_id, []).append(row)

        self.route_index: Dict[str, List[Dict[str, str]]] = {}
        self.route_leg_index: Dict[str, Dict[str, str]] = {}
        for leg in self.route_legs:
            from_port = leg.get("from_port", "")
            if from_port:
                self.route_index.setdefault(from_port, []).append(leg)
            leg_id = leg.get("leg_id", "")
            if leg_id:
                self.route_leg_index[leg_id] = leg

        self.port_region_lookup: Dict[str, str] = {
            str(row.get("port_code", "")).upper(): str(row.get("region", "")).upper()
            for row in self.port_nodes
            if row.get("port_code")
        }

    def _read_csv(self, filename: str) -> List[Dict[str, str]]:
        path = self.dataset_dir / filename
        source_name = Path(filename).stem
        try:
            rows, meta = load_source_rows_with_meta(source_name, path)
            self.source_metadata[source_name] = meta
            return rows
        except FileNotFoundError:
            self.source_metadata[source_name] = {
                "source_name": source_name,
                "source_type": "missing",
                "source_ref": str(path),
                "loaded_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source_version": "missing",
                "row_count": 0,
            }
            return []

    def get_current_port(self, shipment_payload: Dict[str, Dict[str, str]]) -> Optional[str]:
        tos = shipment_payload.get("tos_terminal", {})
        terminal_id = tos.get("terminal_id", "")
        if terminal_id in self.terminal_port_index:
            return self.terminal_port_index[terminal_id]
        return tos.get("port_code") or None

    def get_destination_port(self, shipment_id: str) -> Optional[str]:
        row = self.customer_commitments_index.get(shipment_id, {})
        return row.get("destination_port")

    def get_commitment(self, shipment_id: str) -> Dict[str, str]:
        return self.customer_commitments_index.get(shipment_id, {})

    def get_lane_capacity(self, lane_id: str, preferred_carrier: Optional[str] = None) -> Dict[str, Any]:
        rows = self.lane_capacity_index.get(lane_id, [])
        if not rows:
            return {"available_slots": 0.0, "carrier_id": preferred_carrier or "UNKNOWN", "reliability_score": 0.0}

        filtered = rows
        if preferred_carrier:
            filtered = [r for r in rows if r.get("carrier_id") == preferred_carrier] or rows

        best = max(filtered, key=lambda x: (float(x.get("available_slots", "0") or 0), float(x.get("reliability_score", "0") or 0)))
        return {
            "available_slots": float(best.get("available_slots", "0") or 0),
            "carrier_id": best.get("carrier_id", "UNKNOWN"),
            "reliability_score": float(best.get("reliability_score", "0") or 0),
        }

    def get_earliest_terminal_slot(self, terminal_id: str) -> Optional[Dict[str, str]]:
        slots = [row for row in self.terminal_slots if row.get("terminal_id") == terminal_id and float(row.get("available_slots", "0") or 0) > 0]
        if not slots:
            return None
        return min(slots, key=lambda x: x.get("slot_time_utc", ""))

    def is_port_blocked(self, region: str, port_code: str) -> bool:
        policy = self.policy_index.get(region, {})
        blocked_raw = policy.get("blocked_ports", "")
        if not blocked_raw.strip():
            return False
        blocked = {item.strip() for item in blocked_raw.split("|") if item.strip()}
        return port_code in blocked

    def region_allows_transshipment(self, region: str) -> bool:
        policy = self.policy_index.get(region, {})
        return policy.get("allow_transshipment", "TRUE").upper() == "TRUE"

    def enumerate_route_plans(
        self,
        from_port: str,
        to_port: str,
        region: str,
        max_hops: int = 2,
        scenario: Optional[Dict[str, Any]] = None,
    ) -> List[RoutePlan]:
        if not from_port or not to_port:
            return []

        scenario = scenario or {}
        blocked_modes = {str(x).upper() for x in scenario.get("blocked_modes", [])}
        blocked_regions = {str(x).upper() for x in scenario.get("blocked_regions", [])}
        route_preference = str(scenario.get("route_preference", "FASTEST") or "FASTEST").upper()

        if max_hops < 1:
            max_hops = 1

        if route_preference == "AVOID_TRANSSHIPMENT":
            max_hops = 1

        if not self.region_allows_transshipment(region):
            max_hops = 1

        plans: List[RoutePlan] = []

        def dfs(current_port: str, path_legs: List[Dict[str, str]], visited: set[str]) -> None:
            if len(path_legs) >= max_hops:
                return

            for leg in self.route_index.get(current_port, []):
                next_port = leg.get("to_port", "")
                if not next_port or next_port in visited:
                    continue

                leg_mode = str(leg.get("mode", "") or "").upper()
                if leg_mode and leg_mode in blocked_modes:
                    continue

                from_region = str(self._region_for_port(leg.get("from_port", "")) or "").upper()
                to_region = str(self._region_for_port(next_port) or "").upper()
                if (from_region and from_region in blocked_regions) or (to_region and to_region in blocked_regions):
                    continue

                if self.is_port_blocked(region, next_port):
                    continue

                next_path = path_legs + [leg]
                if next_port == to_port:
                    plans.append(self._plan_from_legs(next_path))
                    continue

                dfs(next_port, next_path, visited | {next_port})

        dfs(from_port, [], {from_port})

        if route_preference == "CHEAPEST":
            plans.sort(key=lambda p: (p.base_cost_usd, p.transit_hours, p.variability_hours))
        elif route_preference == "LOWEST_RISK":
            plans.sort(key=lambda p: (p.variability_hours, p.transit_hours, p.base_cost_usd))
        else:
            plans.sort(key=lambda p: (p.transit_hours, p.base_cost_usd, p.variability_hours))
        return plans[:8]

    def _region_for_port(self, port_code: str) -> str:
        for row in self.port_nodes:
            if row.get("port_code", "") == port_code:
                return row.get("region", "")
        return ""

    def _plan_from_legs(self, legs: List[Dict[str, str]]) -> RoutePlan:
        path_ports = [legs[0].get("from_port", "")]
        leg_ids: List[str] = []
        lane_ids: List[str] = []
        carrier_ids: List[str] = []
        modes: List[str] = []
        transit_hours = 0.0
        variability_hours = 0.0
        base_cost = 0.0

        for leg in legs:
            path_ports.append(leg.get("to_port", ""))
            leg_ids.append(leg.get("leg_id", ""))
            lane_ids.append(leg.get("lane_id", ""))
            carrier_ids.append(leg.get("carrier_id", ""))
            modes.append((leg.get("mode", "UNKNOWN") or "UNKNOWN").upper())
            transit_hours += float(leg.get("avg_transit_hours", "0") or 0)
            variability_hours += float(leg.get("variability_hours", "0") or 0)
            base_cost += float(leg.get("base_cost_usd", "0") or 0)

        return RoutePlan(
            path_ports=path_ports,
            leg_ids=leg_ids,
            lane_ids=lane_ids,
            carrier_ids=carrier_ids,
            transit_hours=transit_hours,
            variability_hours=variability_hours,
            base_cost_usd=base_cost,
            modes=modes,
            mode_breakdown="->".join(modes),
        )

    def evaluate_plan_disruptions(self, plan: RoutePlan, now_utc: datetime, horizon_days: int = 10) -> Dict[str, Any]:
        legs = [self.route_leg_index.get(leg_id, {}) for leg_id in plan.leg_ids]
        legs = [leg for leg in legs if leg]
        return evaluate_route_disruptors(legs, self.macro_disruptors, self.port_region_lookup, now_utc, horizon_days=horizon_days)
