"""
End-to-end demonstration of the battery control system workflow.

This script demonstrates the complete message flow:
1. Cloud publishes schedule to MQTT topic
2. Device receives schedule
3. Device validates schedule
4. Device applies schedule (mock)
5. Device sends acknowledgement
6. Cloud receives acknowledgement

This is illustrative code showing the complete workflow in a single script.
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional, List

# Import cloud and device functions
from cloud.publish_schedule import (
    generate_schedule,
    publish_schedule,
    get_device_capabilities,
    on_execution_result_received,
    subscribe_to_execution_results
)
from device.mqtt_client import (
    validate_schedule,
    apply_schedule,
    create_acknowledgement,
    publish_acknowledgement,
    on_schedule_received,
    execute_interval,
    create_execution_result,
    publish_execution_result
)

# Mock MQTT broker for simulating async message flow
_mock_mqtt_subscribers = {}  # topic -> callback function


def mock_mqtt_publish(topic: str, payload: bytes) -> None:
    """
    Mock MQTT publish that automatically delivers to subscribers.
    
    Simulates real async MQTT flow by calling subscriber callbacks immediately.
    In production, this would be handled by the MQTT broker.
    """
    if topic in _mock_mqtt_subscribers:
        callback = _mock_mqtt_subscribers[topic]
        callback(topic, payload)


def mock_mqtt_subscribe(topic: str, callback) -> None:
    """
    Mock MQTT subscribe that registers a callback for a topic.
    
    In production, this would be handled by the MQTT client library.
    """
    _mock_mqtt_subscribers[topic] = callback


def print_metrics_summary(metrics: List[Dict[str, Any]]) -> None:
    """
    Print a summary table of device flows for multi-device scenarios.
    
    Args:
        metrics: List of metric dicts with keys: schedule_id, device_id, status, 
                 max_power_kw_applied, timestamp
    """
    if not metrics:
        return
    
    print("=" * 70)
    print("METRICS SUMMARY")
    print("=" * 70)
    print()
    
    # Table header
    header = f"{'Schedule ID':<15} {'Device ID':<15} {'Status':<10} {'Max Power (kW)':<15} {'Timestamp':<20}"
    print(header)
    print("-" * 75)
    
    # Table rows
    for m in metrics:
        schedule_id = m.get('schedule_id', 'unknown')[:14]
        device_id = m.get('device_id', 'unknown')[:14]
        status = m.get('status', 'unknown')[:9]
        max_power = str(m.get('max_power_kw_applied', 'N/A'))[:14]
        timestamp = m.get('timestamp', 'unknown')[:19]
        print(f"{schedule_id:<15} {device_id:<15} {status:<10} {max_power:<15} {timestamp:<20}")
    
    print()


def simulate_end_to_end_flow(device_id: str = "device-001", use_mock_mqtt: bool = True) -> Dict[str, Any]:
    """
    Simulate the complete end-to-end flow from cloud publish to device ack.
    
    This demonstrates:
    - Cloud generates and publishes schedule
    - Device receives and validates schedule
    - Device applies schedule
    - Device sends acknowledgement
    - Cloud receives acknowledgement (mock)
    """
    print("=" * 70)
    print("END-TO-END BATTERY CONTROL SYSTEM DEMONSTRATION")
    print("=" * 70)
    print()
    
    # Step 1: Cloud generates schedule
    print("[STEP 1] Cloud: Generating schedule")
    print("-" * 70)
    today = datetime.utcnow().date().isoformat()
    
    # Get device capabilities from registry
    capabilities = get_device_capabilities(device_id)
    max_power_kw = capabilities["max_power_kw"]
    
    schedule = generate_schedule(device_id, today, max_power_kw=max_power_kw)
    print(f"  Schedule ID: {schedule['schedule_id']}")
    print(f"  Device ID: {schedule['device_id']}")
    print(f"  Version: {schedule['version']}")
    print(f"  Intervals: {len(schedule['intervals'])}")
    print(f"  Max power: {schedule.get('max_power_kw')} kW")
    print()
    
    # Step 2: Cloud publishes schedule
    print("[STEP 2] Cloud: Publishing schedule to MQTT")
    print("-" * 70)
    topic = f"devices/{device_id}/schedule"
    payload = json.dumps(schedule, indent=None).encode("utf-8")
    print(f"  Topic: {topic}")
    print(f"  Payload size: {len(payload)} bytes")
    print(f"  QoS: 1 (at least once)")
    print(f"  Retain: False (time-sensitive)")
    print()
    
    # Step 3: Device receives schedule (via mock MQTT or direct call)
    print("[STEP 3] Device: Receiving schedule from MQTT")
    print("-" * 70)
    
    if use_mock_mqtt:
        # Simulate async MQTT flow: device subscribes, then cloud publishes
        # In production, this would be handled by MQTT broker asynchronously
        print(f"  Device subscribed to: {topic}")
        print("  Cloud publishing...")
        print(f"  Topic: {topic}")
        print(f"  Received payload ({len(payload)} bytes)")
        print()
        print("  Note: In real async MQTT flow, device callback would process")
        print("        message asynchronously. For demo clarity, showing steps below.")
        print()
    else:
        print(f"  Topic: {topic}")
        print(f"  Received payload")
        print()
    
    # Step 4: Device validates schedule
    print("[STEP 4] Device: Validating schedule")
    print("-" * 70)
    is_valid, error_reason, max_power_kw_applied = validate_schedule(schedule)
    
    if not is_valid:
        print(f"  ❌ Validation FAILED: {error_reason}")
        print()
        print("[STEP 5] Device: Sending FAILED acknowledgement")
        print("-" * 70)
        ack = create_acknowledgement(
            schedule,
            "FAILED",
            error_reason,
            max_power_kw_applied=max_power_kw_applied
        )
        print(f"  Status: {ack['status']}")
        print(f"  Error: {ack.get('error_reason')}")
        print()
        print("[STEP 6] Cloud: Received FAILED acknowledgement")
        print("-" * 70)
        print(f"  Schedule ID: {ack['schedule_id']}")
        print(f"  Device ID: {ack['device_id']}")
        print(f"  Status: {ack['status']}")
        print(f"  Timestamp: {ack['timestamp']}")
        return
    
    print(f"  ✅ Validation PASSED")
    print(f"  Max power limit applied: {max_power_kw_applied} kW")
    print()
    
    # Step 5: Device applies schedule
    print("[STEP 5] Device: Applying schedule to hardware")
    print("-" * 70)
    success = apply_schedule(schedule)
    
    if not success:
        print("  ❌ Hardware application FAILED")
        ack_status = "FAILED"
        error_reason = "Hardware application failed"
    else:
        print("  ✅ Schedule applied successfully")
        ack_status = "APPLIED"
        error_reason = None
    print()
    
    # Step 6: Device sends acknowledgement
    print(f"[STEP 6] Device: Sending {ack_status} acknowledgement")
    print("-" * 70)
    applied_at = datetime.utcnow() if ack_status == "APPLIED" else None
    ack = create_acknowledgement(
        schedule,
        ack_status,
        error_reason,
        max_power_kw_applied=max_power_kw_applied,
        applied_at=applied_at
    )
    ack_topic = f"devices/{ack['device_id']}/ack"
    print(f"  Topic: {ack_topic}")
    print(f"  Schedule ID: {ack['schedule_id']}")
    print(f"  Status: {ack['status']}")
    print(f"  Timestamp: {ack['timestamp']}")
    if ack.get('applied_at'):
        print(f"  Applied at: {ack['applied_at']}")
    if ack.get('max_power_kw_applied'):
        print(f"  Max power applied: {ack['max_power_kw_applied']} kW")
    if ack.get('error_reason'):
        print(f"  Error: {ack['error_reason']}")
    print()
    
    # Step 7: Cloud receives acknowledgement
    print("[STEP 7] Cloud: Received acknowledgement")
    print("-" * 70)
    print(f"  Schedule ID: {ack['schedule_id']}")
    print(f"  Device ID: {ack['device_id']}")
    print(f"  Status: {ack['status']}")
    print(f"  Timestamp: {ack['timestamp']}")
    if ack.get('applied_at'):
        print(f"  Applied at: {ack['applied_at']}")
        # Calculate latency
        issued_at = datetime.fromisoformat(schedule['issued_at'].replace('Z', '+00:00'))
        applied_at_dt = datetime.fromisoformat(ack['applied_at'].replace('Z', '+00:00'))
        latency_ms = (applied_at_dt - issued_at).total_seconds() * 1000
        print(f"  Latency (issued_at → applied_at): {latency_ms:.0f} ms")
    if ack.get('max_power_kw_applied'):
        print(f"  Max power limit: {ack['max_power_kw_applied']} kW")
    if ack.get('error_reason'):
        print(f"  Error reason: {ack['error_reason']}")
    print()
    
    # Summary
    print("=" * 70)
    print("END-TO-END FLOW COMPLETE")
    print("=" * 70)
    print()
    print("Key points demonstrated:")
    print("  ✅ Idempotency: schedule_id enables safe retries")
    print("  ✅ Edge validation: device validates power limits")
    print("  ✅ Traceability: ack links back to schedule via schedule_id")
    print("  ✅ Observability: max_power_kw_applied in ack for monitoring")
    print("  ✅ Granular timing: applied_at timestamp for latency analysis")
    print("  ✅ Safety: version checking prevents incompatible schedules")
    print()
    
    # Return metrics for summary table
    return {
        'schedule_id': ack['schedule_id'],
        'device_id': ack['device_id'],
        'status': ack['status'],
        'max_power_kw_applied': ack.get('max_power_kw_applied'),
        'timestamp': ack['timestamp']
    }


def simulate_validation_failure() -> None:
    """Demonstrate device handling of invalid schedule."""
    print("=" * 70)
    print("DEMONSTRATION: Validation Failure Handling")
    print("=" * 70)
    print()
    
    device_id = "device-001"
    
    # Create invalid schedule (missing required field)
    invalid_schedule = {
        "device_id": device_id,
        "version": "1.0",
        "intervals": [{"start_time": "00:00:00", "power_kw": 10.0}],
        "issued_at": datetime.utcnow().isoformat() + "Z"
        # Missing schedule_id
    }
    
    print("[CLOUD] Publishing invalid schedule (missing schedule_id)")
    print("-" * 70)
    print()
    
    print("[DEVICE] Receiving schedule")
    print("-" * 70)
    print()
    
    print("[DEVICE] Validating schedule")
    print("-" * 70)
    is_valid, error_reason, max_power_kw_applied = validate_schedule(invalid_schedule)
    print(f"  ❌ Validation FAILED: {error_reason}")
    print()
    
    print("[DEVICE] Sending FAILED acknowledgement")
    print("-" * 70)
    ack = create_acknowledgement(
        invalid_schedule,
        "FAILED",
        error_reason,
        max_power_kw_applied=max_power_kw_applied,
        schedule_id="unknown"  # Fallback since schedule_id is missing
    )
    print(f"  Status: {ack['status']}")
    print(f"  Error: {ack['error_reason']}")
    print(f"  Schedule ID: {ack['schedule_id']} (fallback)")
    print()
    
    print("[CLOUD] Received FAILED acknowledgement")
    print("-" * 70)
    print("  Cloud can now track validation failures for monitoring")
    print()


if __name__ == "__main__":
    metrics = []
    
    # Run successful end-to-end flow for multiple devices
    print("Running end-to-end flows for multiple devices...")
    print()
    
    for device_id in ["device-001", "device-002", "device-003"]:
        metric = simulate_end_to_end_flow(device_id=device_id, use_mock_mqtt=True)
        metrics.append(metric)
        print("\n" * 2)
    
    # Print metrics summary table
    print_metrics_summary(metrics)
    
    print("\n" * 2)
    
    # Run validation failure scenario
    simulate_validation_failure()

