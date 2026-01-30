"""
Example cloud service that publishes battery schedules to edge devices via MQTT.

This is illustrative code demonstrating:
- Topic naming conventions
- Message serialization
- Idempotency via schedule_id
- QoS selection rationale

NOT production-ready: lacks proper error handling, configuration management,
authentication, retry logic, and observability instrumentation.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any

# In production, use a real MQTT client library (e.g., paho-mqtt)
# from paho.mqtt import client as mqtt_client


def generate_schedule(device_id: str, date: str) -> Dict[str, Any]:
    """
    Generate a mock daily battery schedule.
    
    In production, this would:
    - Query optimization algorithms
    - Consider grid conditions
    - Account for device capabilities
    - Handle timezone conversions
    """
    intervals = []
    
    # Generate 48 half-hour intervals (24 hours)
    for hour in range(24):
        for minute in [0, 30]:
            start_time = f"{hour:02d}:{minute:02d}:00"
            # Mock logic: charge during off-peak (2-6 AM), discharge during peak (6-9 PM)
            if 2 <= hour < 6:
                power_kw = 10.0  # Charge during off-peak
            elif 18 <= hour < 21:
                power_kw = -15.0  # Discharge during peak
            else:
                power_kw = 0.0  # Idle otherwise
            
            intervals.append({
                "start_time": start_time,
                "power_kw": power_kw
            })
    
    return {
        "schedule_id": date,  # Date-based ID enables idempotent retries
        "device_id": device_id,
        "version": "1.0",  # Schema version for protocol evolution
        "intervals": intervals,
        "issued_at": datetime.utcnow().isoformat() + "Z"
    }


def publish_schedule(device_id: str, schedule: Dict[str, Any], mqtt_client=None) -> None:
    """
    Publish a schedule to the device-specific MQTT topic.
    
    Topic naming: devices/{device_id}/schedule
    - Enables per-device subscriptions (devices only subscribe to their own topic)
    - Supports horizontal scaling (broker can partition by device_id)
    - Clear ownership: each device has a dedicated topic
    
    QoS Level 1 (At least once):
    - Ensures delivery even if device temporarily disconnects
    - Prevents schedule loss during network hiccups
    - Acceptable duplicate handling: schedule_id enables idempotency
    - Alternative: QoS 2 (exactly once) if duplicates are unacceptable, but adds overhead
    
    Idempotency:
    - schedule_id allows safe retries: device can ignore duplicate schedule_ids
    - Cloud can republish on timeout without causing double-application
    - Critical for reliability in distributed systems
    """
    topic = f"devices/{device_id}/schedule"
    payload = json.dumps(schedule, indent=None)  # Compact JSON for efficiency
    
    print(f"[CLOUD] Publishing schedule to {topic}")
    print(f"[CLOUD] Schedule ID: {schedule['schedule_id']}")
    print(f"[CLOUD] Payload size: {len(payload)} bytes")
    
    # In production, use real MQTT client:
    # mqtt_client.publish(topic, payload, qos=1, retain=False)
    # 
    # retain=False: Schedules are time-sensitive; we don't want devices
    #               applying stale schedules after reconnection.
    #               Fresh schedules should be published on device reconnect.
    
    # In production, also:
    # - Log to observability system (schedule_id, device_id, timestamp)
    # - Track publish success/failure for monitoring
    # - Implement retry logic with exponential backoff
    # - Handle broker connection failures gracefully
    # - Use TLS for secure transport
    # - Authenticate with broker credentials


def main():
    """
    Example: Publish a schedule for a device.
    
    In production, this would be triggered by:
    - Scheduled job (daily schedule generation)
    - Event-driven (grid condition changes)
    - API endpoint (manual override)
    """
    device_id = "raspberry-pi-001"
    today = datetime.utcnow().date().isoformat()
    
    schedule = generate_schedule(device_id, today)
    
    # Validate against schema (in production, use jsonschema library)
    print(f"[CLOUD] Generated schedule for {device_id}")
    print(f"[CLOUD] Schedule version: {schedule['version']}")
    print(f"[CLOUD] Intervals: {len(schedule['intervals'])}")
    
    # Publish to MQTT
    publish_schedule(device_id, schedule)
    
    print("[CLOUD] Schedule published (mock)")


if __name__ == "__main__":
    main()

