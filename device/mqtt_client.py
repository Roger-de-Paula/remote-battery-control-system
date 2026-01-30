"""
Example edge device MQTT client that subscribes to schedules and sends acknowledgements.

This is illustrative code demonstrating:
- Device-specific topic subscription
- Message validation and version checking
- Safety-first approach (reject unknown versions)
- Acknowledgement publishing for traceability

NOT production-ready: lacks proper error handling, reconnection logic,
configuration management, security, and production-grade MQTT client usage.
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

# In production, use a real MQTT client library (e.g., paho-mqtt)
# from paho.mqtt import client as mqtt_client


# Supported schema versions - devices reject unknown versions for safety
SUPPORTED_VERSIONS = {"1.0", "1.1"}


def validate_schedule(schedule: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Validate incoming schedule message.
    
    Returns: (is_valid, error_reason)
    
    Safety principle: Reject unknown versions to prevent applying incompatible
    schedules that could damage hardware or cause unexpected behavior.
    """
    # Check required fields
    required_fields = ["schedule_id", "device_id", "version", "intervals", "issued_at"]
    for field in required_fields:
        if field not in schedule:
            return False, f"Missing required field: {field}"
    
    # Version check - CRITICAL for safety
    version = schedule["version"]
    if version not in SUPPORTED_VERSIONS:
        return False, f"Unknown schema version: {version}. Supported: {SUPPORTED_VERSIONS}"
    
    # Validate intervals structure
    intervals = schedule["intervals"]
    if not isinstance(intervals, list) or len(intervals) == 0:
        return False, "intervals must be a non-empty array"
    
    if len(intervals) > 48:
        return False, f"Too many intervals: {len(intervals)} (max 48)"
    
    # Validate each interval
    for i, interval in enumerate(intervals):
        if "start_time" not in interval or "power_kw" not in interval:
            return False, f"Interval {i} missing required fields"
        
        power_kw = interval["power_kw"]
        if not isinstance(power_kw, (int, float)):
            return False, f"Interval {i}: power_kw must be numeric"
        
        # Device-specific safety limits (adjust per device model)
        if abs(power_kw) > 50:
            return False, f"Interval {i}: power_kw {power_kw} exceeds device limit (±50 kW)"
    
    return True, None


def apply_schedule(schedule: Dict[str, Any]) -> bool:
    """
    Apply schedule to device hardware.
    
    In production, this would:
    - Interface with battery controller (e.g., via serial, Modbus, or device SDK)
    - Set power setpoints for each interval
    - Verify hardware acknowledges commands
    - Handle hardware errors gracefully
    
    Returns: True if successful, False otherwise
    """
    print(f"[DEVICE] Applying schedule {schedule['schedule_id']}")
    print(f"[DEVICE] Intervals to apply: {len(schedule['intervals'])}")
    
    # Mock: In production, send commands to battery controller
    # for interval in schedule['intervals']:
    #     battery_controller.set_power(interval['start_time'], interval['power_kw'])
    
    print("[DEVICE] Schedule applied successfully (mock)")
    return True


def create_acknowledgement(
    schedule: Dict[str, Any],
    status: str,
    error_reason: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create an acknowledgement message to send back to cloud.
    
    Status values:
    - RECEIVED: Message received and validated (but not yet applied)
    - APPLIED: Schedule successfully applied to device
    - FAILED: Schedule could not be applied (see error_reason)
    
    Traceability: schedule_id links this ack to the original schedule,
    enabling cloud to track end-to-end request lifecycle.
    """
    ack = {
        "schedule_id": schedule["schedule_id"],
        "device_id": schedule["device_id"],
        "status": status,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    if status == "FAILED" and error_reason:
        ack["error_reason"] = error_reason
    
    return ack


def publish_acknowledgement(ack: Dict[str, Any], mqtt_client=None) -> None:
    """
    Publish acknowledgement message to cloud.
    
    Topic naming: devices/{device_id}/ack
    - Separate topic from schedules for clear separation of concerns
    - Enables cloud to subscribe to all acks or device-specific acks
    - Supports different QoS levels if needed
    
    QoS Level 1 (At least once):
    - Ensures cloud receives ack even during network issues
    - Acceptable duplicates: cloud can deduplicate by (schedule_id, device_id, timestamp)
    - Critical for observability: cloud needs to know device status
    """
    topic = f"devices/{ack['device_id']}/ack"
    payload = json.dumps(ack, indent=None)
    
    print(f"[DEVICE] Publishing acknowledgement to {topic}")
    print(f"[DEVICE] Status: {ack['status']}")
    
    # In production, use real MQTT client:
    # mqtt_client.publish(topic, payload, qos=1, retain=False)
    
    # In production, also:
    # - Log to local observability system
    # - Handle publish failures (retry, local queue)
    # - Use TLS for secure transport
    # - Authenticate with broker credentials


def on_schedule_received(topic: str, payload: bytes, device_id: str) -> None:
    """
    Handle incoming schedule message from MQTT broker.
    
    This function demonstrates the device-side message processing flow:
    1. Deserialize JSON
    2. Validate schema and version
    3. Send RECEIVED ack (optional, for early confirmation)
    4. Apply schedule to hardware
    5. Send APPLIED or FAILED ack
    
    Separation of concerns:
    - Validation is separate from application
    - Acknowledgement is separate from processing
    - Each step can fail independently and be handled appropriately
    """
    try:
        schedule = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # Invalid JSON - can't create proper ack without schedule_id
        print(f"[DEVICE] ERROR: Failed to parse message: {e}")
        return
    
    print(f"[DEVICE] Received schedule on topic: {topic}")
    print(f"[DEVICE] Schedule ID: {schedule.get('schedule_id', 'unknown')}")
    
    # Validate schedule
    is_valid, error_reason = validate_schedule(schedule)
    
    if not is_valid:
        print(f"[DEVICE] Validation failed: {error_reason}")
        ack = create_acknowledgement(schedule, "FAILED", error_reason)
        publish_acknowledgement(ack)
        return
    
    # Optional: Send RECEIVED ack for early confirmation
    # ack_received = create_acknowledgement(schedule, "RECEIVED")
    # publish_acknowledgement(ack_received)
    
    # Apply schedule to hardware
    try:
        success = apply_schedule(schedule)
        
        if success:
            ack = create_acknowledgement(schedule, "APPLIED")
        else:
            ack = create_acknowledgement(schedule, "FAILED", "Hardware application failed")
        
        publish_acknowledgement(ack)
        
    except Exception as e:
        # Hardware error - send FAILED ack with error context
        print(f"[DEVICE] ERROR applying schedule: {e}")
        ack = create_acknowledgement(schedule, "FAILED", f"Hardware error: {str(e)}")
        publish_acknowledgement(ack)


def subscribe_to_schedules(device_id: str, mqtt_client=None) -> None:
    """
    Subscribe to device-specific schedule topic.
    
    Topic: devices/{device_id}/schedule
    - Each device only subscribes to its own topic (reduces message volume)
    - Enables broker-level filtering and scaling
    - Clear ownership: device knows exactly which messages are for it
    
    QoS Level 1:
    - Matches publisher QoS for consistent delivery guarantees
    - Ensures device receives schedule even if temporarily disconnected
    - Acceptable duplicates: schedule_id enables idempotency
    
    In production, also implement:
    - Automatic reconnection with exponential backoff
    - Connection state monitoring
    - Last Will and Testament (LWT) for device health reporting
    - Keep-alive configuration appropriate for network conditions
    """
    topic = f"devices/{device_id}/schedule"
    
    print(f"[DEVICE] Subscribing to {topic}")
    
    # In production, use real MQTT client:
    # mqtt_client.subscribe(topic, qos=1)
    # mqtt_client.on_message = lambda client, userdata, msg: on_schedule_received(
    #     msg.topic, msg.payload, device_id
    # )
    
    # In production, also:
    # - Handle subscription failures
    # - Monitor subscription status
    # - Implement re-subscription on reconnect
    # - Use TLS for secure transport
    # - Authenticate with broker credentials


def main():
    """
    Example: Device subscribes to schedules and processes them.
    
    In production, this would:
    - Run as a systemd service or container
    - Load configuration from environment or config file
    - Connect to MQTT broker with proper credentials
    - Implement health checks and graceful shutdown
    - Handle reconnection and network failures
    - Integrate with device hardware SDK
    """
    device_id = "raspberry-pi-001"
    
    print(f"[DEVICE] Starting MQTT client for device: {device_id}")
    
    # In production, initialize MQTT client:
    # mqtt_client = mqtt_client.Client(client_id=device_id)
    # mqtt_client.connect(broker_host, broker_port, keepalive=60)
    # mqtt_client.loop_start()
    
    # Subscribe to schedules
    subscribe_to_schedules(device_id)
    
    # In production, keep running:
    # try:
    #     while True:
    #         time.sleep(1)
    # except KeyboardInterrupt:
    #     mqtt_client.loop_stop()
    #     mqtt_client.disconnect()
    
    print("[DEVICE] Device client running (mock)")


if __name__ == "__main__":
    main()

