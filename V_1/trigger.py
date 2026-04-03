import csv
import os
import json
from pathlib import Path

# Provide the base directory path for datasets
BASE_DIR = Path(r"c:\Users\LENOVO\OneDrive\Desktop\BITS\Hack\datasets")

# -------------------------------------------------------------------
# REAL-WORLD THRESHOLD CONFIGURATION
# -------------------------------------------------------------------
# Defines exactly what is considered a "breach" based on real-world
# operational tolerances in supply chain and logistics.
THRESHOLDS = {
    "tos_terminal.csv": {
        "yard_occupancy_pct": {"op": ">", "val": 85.0, "severity": "CRITICAL", "msg": "Yard occupancy exceeded 85%. Risk of gridlock."},
        "yard_occupancy_rate_of_change": {"op": ">", "val": 5.0, "severity": "HIGH", "msg": "Yard filling rapidly (>5%/hr). Predicts imminent congestion."},
        "container_dwell_time_hrs": {"op": ">", "val": 48.0, "severity": "CRITICAL", "msg": "Container dwell exeeded 48h. Demurrage accruing."},
        "crane_plan_execution_pct": {"op": "<", "val": 75.0, "severity": "MEDIUM", "msg": "Crane execution <75%. Vessel turnaround delayed."},
        "crane_downtime_min": {"op": ">", "val": 30.0, "severity": "CRITICAL", "msg": "Crane unplanned downtime >30min. Fast escalation needed."},
        "gate_throughput_trucks_per_hr": {"op": "<", "val": 70.0, "severity": "HIGH", "msg": "Gate bottleneck forming. Truck throughput degraded."},
        "vessel_departure_delay_min": {"op": ">", "val": 60.0, "severity": "HIGH", "msg": "Vessel delayed by over 1 hour."},
        "cargo_hold_flag": {"op": "==", "val": "TRUE", "severity": "HIGH", "msg": "Cargo is on hold at terminal."}
    },
    "tms_transport.csv": {
        "delivery_delay_min": {"op": ">", "val": 30.0, "severity": "HIGH", "msg": "Delivery delay exceeded 30 mins. Customer SLA at risk."},
        "carrier_reliability_score": {"op": "<", "val": 0.70, "severity": "MEDIUM", "msg": "Carrier reliability dropped below 70% threshold."},
        "vehicle_breakdown_flag": {"op": "==", "val": "TRUE", "severity": "CRITICAL", "msg": "Vehicle breakdown reported! Rerouting required immediately."},
        "gate_queue_depth": {"op": ">", "val": 30.0, "severity": "HIGH", "msg": "Truck gate queue > 30. Demurrage and driver detention risk."},
        "unassigned_shipments_count": {"op": ">", "val": 10.0, "severity": "CRITICAL", "msg": "High unassigned shipment volume! Immediate spot booking required."},
        "truck_slot_fill_rate_pct": {"op": "<", "val": 40.0, "severity": "MEDIUM", "msg": "Inefficient truck loading. Deadhead/empty cost risk."},
        "driver_hours_of_service_remaining": {"op": "<", "val": 2.0, "severity": "CRITICAL", "msg": "Driver HOS limit approaching! Swap driver to avoid delay."},
        "spot_market_rate_spike_pct": {"op": ">", "val": 30.0, "severity": "HIGH", "msg": "Spot rate spike > 30%. Transportation budget overrun risk."}
    },
    "wms_warehouse.csv": {
        "picking_backlog_hours": {"op": ">", "val": 4.0, "severity": "HIGH", "msg": "Picking backlog over 4 hours. Dispatch slots in jeopardy."},
        "dock_slot_availability": {"op": "==", "val": 0.0, "severity": "CRITICAL", "msg": "0 Dock slots available. Trucks will face detention charges."},
        "shift_throughput_units_per_hr": {"op": "<", "val": 100.0, "severity": "MEDIUM", "msg": "Shift throughput lagging below target baseline."},
        "dispatch_slot_missed_flag": {"op": "==", "val": "TRUE", "severity": "HIGH", "msg": "Dispatch slot missed. Re-booking with carrier required."},
        "inventory_readiness_pct": {"op": "<", "val": 80.0, "severity": "HIGH", "msg": "Inventory readiness < 80%. Risk of incomplete final dispatch."}
    },
    "customs_compliance.csv": {
        "clearance_duration_hrs": {"op": ">", "val": 24.0, "severity": "HIGH", "msg": "Clearance pending > 24 hrs. Risk of heavy terminal detention."},
        "document_completeness_pct": {"op": "<", "val": 100.0, "severity": "CRITICAL", "msg": "Documents incomplete before customs submission deadline."},
        "inspection_flag": {"op": "==", "val": "TRUE", "severity": "HIGH", "msg": "Shipment pulled for intensive customs inspection."},
        "holiday_proximity_flag": {"op": "==", "val": "TRUE", "severity": "MEDIUM", "msg": "Upcoming holiday closure. Pre-clearance highly urgent."},
        "sanctions_screening_flag": {"op": "==", "val": "TRUE", "severity": "CRITICAL", "msg": "Sanctions screening hit! Hard legal stop."},
        "hs_code_mismatch_flag": {"op": "==", "val": "TRUE", "severity": "CRITICAL", "msg": "HS Code mismatch detected. Immediate correction required."}
    },
    "erp_finance.csv": {
        "free_time_expiry_hrs_remaining": {"op": "<", "val": 12.0, "severity": "CRITICAL", "msg": "Free time expires in < 12 hrs. Act immediately to avoid Demurrage."},
        "demurrage_accrual_usd": {"op": ">", "val": 500.0, "severity": "HIGH", "msg": "Demurrage charges have exceeded $500."},
        "sla_breach_probability_pct": {"op": ">", "val": 60.0, "severity": "HIGH", "msg": "SLA breach probability > 60%. Prioritize intervention."},
        "time_to_sla_breach_hrs": {"op": "<", "val": 4.0, "severity": "CRITICAL", "msg": "Customer SLA breach imminent in < 4 hours!"}
    },
    "logistics_visibility.csv": {
        "transhipment_missed_flag": {"op": "==", "val": "TRUE", "severity": "CRITICAL", "msg": "Transhipment connection missed. Cargo stranded at hub."},
        "load_completion_pct": {"op": "<", "val": 80.0, "severity": "HIGH", "msg": "Vessel load completion <80% near ETD. Cargo rolled risk."}
    },
    "iot_telemetry.csv": {
        "temperature_exceedance_duration_min": {"op": ">", "val": 30.0, "severity": "CRITICAL", "msg": "Temperature bounds broken for >30m! Perishable spoilage risk."}
    }
}

def evaluate_condition(actual_val_str, op, threshold_val):
    if not actual_val_str or actual_val_str.strip() == "-" or actual_val_str.strip() == "":
        return False
    
    # Handle string boolean flags quickly
    if op == "==":
        return actual_val_str.strip().upper() == str(threshold_val).upper()
    
    # Handle numeric evaluations
    try:
        actual = float(actual_val_str)
        thresh = float(threshold_val)
        
        if op == ">": return actual > thresh
        if op == "<": return actual < thresh
        if op == ">=": return actual >= thresh
        if op == "<=": return actual <= thresh
    except ValueError:
        pass # Ignore failed parsing
        
    return False

def generate_alerts():
    alerts = []
    
    if not BASE_DIR.exists():
        print(f"Error: Datasets directory not found at {BASE_DIR}")
        return
        
    # Iterate through the files defined in THRESHOLDS
    for filename, rules in THRESHOLDS.items():
        filepath = BASE_DIR / filename
        
        if not filepath.exists():
            print(f"Warning: File {filename} not found in {BASE_DIR}")
            continue
            
        with open(filepath, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Check each threshold rule against the current row
                for field, rule in rules.items():
                    if field in row:
                        val = row[field]
                        is_breach = evaluate_condition(val, rule["op"], rule["val"])
                        
                        if is_breach:
                            # Build the alert payload
                            alert = {
                                "source": filename.split('.')[0],
                                "timestamp": row.get("timestamp", "UNKNOWN_TIME"),
                                "entity": row.get("shipment_id", row.get("container_id", "UNKNOWN")),
                                "parameter": field,
                                "actual_value": val,
                                "threshold_limit": f"{rule['op']} {rule['val']}",
                                "severity": rule["severity"],
                                "message": rule["msg"]
                            }
                            alerts.append(alert)
                            
    return alerts

class Colors:
    CRITICAL = '\033[91m' # Red
    HIGH = '\033[33m'     # Orange/Yellowish
    MEDIUM = '\033[93m'   # Bright Yellow
    GREEN = '\033[92m'    # Green
    RESET = '\033[0m'     # Reset formatting

if __name__ == "__main__":
    import os
    os.system('') # Enables ANSI color support on Windows terminal
    
    print("-" * 50)
    print(">>> AI CONTROL TOWER TRIGGER ENGINE <<<")
    print("-" * 50)
    
    triggered_alerts = generate_alerts()
    
    if triggered_alerts:
        print(f"{Colors.CRITICAL}!!! Detected {len(triggered_alerts)} alerts based on real-world thresholds!{Colors.RESET}\n")
        for alert in triggered_alerts:
            severity = alert['severity']
            if severity == 'CRITICAL':
                color = Colors.CRITICAL
            elif severity == 'HIGH':
                color = Colors.HIGH
            else:
                color = Colors.MEDIUM
                
            severity_tag = f"{color}[{severity}]{Colors.RESET}"
                
            print(f"[{alert['timestamp']}] {severity_tag} | Source: {alert['source'].upper()}")
            print(f"   Entity: {alert['entity']}")
            print(f"   Trigger: {alert['parameter']} = {alert['actual_value']} (Limit: {alert['threshold_limit']})")
            print(f"   Action: {color}{alert['message']}{Colors.RESET}")
            print("-" * 50)
    else:
        print(f"{Colors.GREEN}OK: System is green. No KPI thresholds breached.{Colors.RESET}")
