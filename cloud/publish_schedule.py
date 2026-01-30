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

# In production, use structured logging (e.g., structlog, python-json-logger)
# import logging
# logger = logging.getLogger(__name__)

# In production, use jsonschema for validation
# import jsonschema
# from schemas.schedule import schedule_schema


def get_device_capabilities(device_id: str) -> Dict[str, Any]:
    """
    Get device capabilities from registry.
    
    In production, this would:
    - Query device registry/database for device model and capabilities
    - Return max_power_kw, supported versions, timezone, etc.
    - Handle device not found errors
    
    Returns:
        Dict with device capabilities (e.g., {"max_power_kw": 50.0, "model": "MAX300"})
    """
    # Mock: In production, query device registry
    # device = device_registry.get(device_id)
    # return {"max_power_kw": device.max_power_kw, "model": device.model}
    return {"max_power_kw": 50.0, "model": "MAX300"}


def generate_schedule(device_id: str, date: str, max_power_kw: float = 50.0) -> Dict[str, Any]:
    """
    Generate a mock daily battery schedule.
    
    In production, this would:
    - Query optimization algorithms
    - Consider grid conditions
    - Account for device capabilities (max_power_kw per device model)
    - Handle timezone conversions
    
    Args:
        device_id: Target device identifier
        date: Schedule date (used for schedule_id)
        max_power_kw: Optional device-specific maximum power limit (default 50 kW)
                      In production, this comes from device registry via get_device_capabilities()
    """
    intervals = []
    
    # Generate 48 half-hour intervals (24 hours)
    for hour in range(24):
        for minute in [0, 30]:
            start_time = f"{hour:02d}:{minute:02d}:00"
            # Mock logic: charge during off-peak (2-6 AM), discharge during peak (6-9 PM)
            # Ensure power_kw values respect device limits
            if 2 <= hour < 6:
                power_kw = min(10.0, max_power_kw * 0.2)  # Charge during off-peak
            elif 18 <= hour < 21:
                power_kw = max(-15.0, -max_power_kw * 0.3)  # Discharge during peak
            else:
                power_kw = 0.0  # Idle otherwise
            
            # Derive mode from power_kw sign (for dashboard visualization)
            mode = "CHARGE" if power_kw > 0 else "DISCHARGE" if power_kw < 0 else "IDLE"
            
            intervals.append({
                "start_time": start_time,
                "power_kw": power_kw,
                "mode": mode  # Optional field for dashboard visualization; power_kw is source of truth
            })
    
    schedule = {
        "schedule_id": date,  # Date-based ID enables idempotent retries
        "device_id": device_id,
        "version": "1.0",  # Schema version for protocol evolution
        "intervals": intervals,
        "issued_at": datetime.utcnow().isoformat() + "Z"
    }
    
    # Optional: include device-specific power limit for edge validation
    # Devices will reject intervals where abs(power_kw) > max_power_kw
    schedule["max_power_kw"] = max_power_kw
    
    return schedule


def validate_schedule_schema(schedule: Dict[str, Any]) -> bool:
    """
    Validate schedule against JSON schema before publishing.
    
    In production, use jsonschema library:
    - jsonschema.validate(schedule, schedule_schema)
    - Raises ValidationError if invalid
    - Prevents publishing malformed schedules that devices would reject
    """
    # Mock validation - in production, use jsonschema.validate()
    required_fields = ["schedule_id", "device_id", "version", "intervals", "issued_at"]
    for field in required_fields:
        if field not in schedule:
            print(f"[CLOUD] Validation failed: missing required field '{field}'")
            return False
    return True


def publish_schedule(device_id: str, schedule: Dict[str, Any], mqtt_client=None) -> bool:
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
    
    Retention policy:
    - retain=False: Schedules are time-sensitive; we don't want devices
      applying stale schedules after reconnection.
      Fresh schedules should be published on device reconnect.
    
    Idempotency:
    - schedule_id allows safe retries: device can ignore duplicate schedule_ids
    - Cloud can republish on timeout without causing double-application
    - Critical for reliability in distributed systems
    
    Returns:
        True if publish succeeded (or mocked), False otherwise
    """
    # Validate schema before publishing
    if not validate_schedule_schema(schedule):
        return False
    
    topic = f"devices/{device_id}/schedule"
    payload = json.dumps(schedule, indent=None)  # Compact JSON for efficiency
    
    # In production, use structured logging:
    # logger.info("publishing_schedule", schedule_id=schedule['schedule_id'], 
    #             device_id=device_id, payload_size=len(payload))
    print(f"[CLOUD] Publishing schedule to {topic}")
    print(f"[CLOUD] Schedule ID: {schedule['schedule_id']}")
    print(f"[CLOUD] Payload size: {len(payload)} bytes")
    
    # In production, use real MQTT client with retry logic:
    # max_retries = 3
    # for attempt in range(max_retries):
    #     try:
    #         result = mqtt_client.publish(topic, payload, qos=1, retain=False)
    #         result.wait_for_publish(timeout=5)
    #         if result.rc == 0:
    #             logger.info("schedule_published", schedule_id=schedule['schedule_id'])
    #             return True
    #     except Exception as e:
    #         wait_time = 2 ** attempt  # Exponential backoff
    #         logger.warning("publish_retry", attempt=attempt+1, error=str(e), wait_time=wait_time)
    #         time.sleep(wait_time)
    # logger.error("publish_failed", schedule_id=schedule['schedule_id'], max_retries=max_retries)
    # return False
    
    # Mock publish
    print(f"[CLOUD] Schedule published (mock)")
    return True


def main():
    """
    Example: Publish a schedule for a device.
    
    In production, this would be triggered by:
    - Scheduled job (daily schedule generation)
    - Event-driven (grid condition changes)
    - API endpoint (manual override)
    
    Production considerations:
    - Device registry integration: Pull max_power_kw from get_device_capabilities()
    - Schema validation: Use jsonschema.validate() before publishing
    - Retry logic: Implement exponential backoff for publish failures
    - Security: Use TLS + client auth or token-based auth for MQTT broker
    - Structured logging: Replace prints with structured logs for observability
    """
    device_id = "device-001"
    today = datetime.utcnow().date().isoformat()
    
    # Get device capabilities from registry (in production)
    capabilities = get_device_capabilities(device_id)
    max_power_kw = capabilities["max_power_kw"]
    
    # Generate schedule with device-specific power limit
    schedule = generate_schedule(device_id, today, max_power_kw=max_power_kw)
    
    print(f"[CLOUD] Generated schedule for {device_id}")
    print(f"[CLOUD] Schedule version: {schedule['version']}")
    print(f"[CLOUD] Intervals: {len(schedule['intervals'])}")
    print(f"[CLOUD] Max power limit: {schedule.get('max_power_kw', 'not specified')} kW")
    
    # Publish to MQTT (includes schema validation)
    success = publish_schedule(device_id, schedule)
    
    if not success:
        print("[CLOUD] Failed to publish schedule")


if __name__ == "__main__":
    main()

