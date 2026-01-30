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

# In production, use structured logging (e.g., structlog, python-json-logger)
# import logging
# logger = logging.getLogger(__name__)
# Structured logging enables cloud monitoring tools to parse and aggregate logs


# Supported schema versions - devices reject unknown versions for safety
SUPPORTED_VERSIONS = {"1.0", "1.1"}


def validate_schedule(schedule: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[float]]:
    """
    Validate incoming schedule message.
    
    Returns: (is_valid, error_reason, max_power_kw_applied)
    
    Safety principle: Reject unknown versions to prevent applying incompatible
    schedules that could damage hardware or cause unexpected behavior.
    
    Edge validation: Devices must reject schedules with power_kw values outside
    allowed range. This prevents hardware damage and ensures schedules match
    device capabilities.
    """
    # Check required fields
    required_fields = ["schedule_id", "device_id", "version", "intervals", "issued_at"]
    for field in required_fields:
        if field not in schedule:
            return False, f"Missing required field: {field}", None
    
    # Version check - CRITICAL for safety
    version = schedule["version"]
    if version not in SUPPORTED_VERSIONS:
        return False, f"Unknown schema version: {version}. Supported: {SUPPORTED_VERSIONS}", None
    
    # Validate intervals structure
    intervals = schedule["intervals"]
    if not isinstance(intervals, list) or len(intervals) == 0:
        return False, "intervals must be a non-empty array", None
    
    if len(intervals) > 48:
        return False, f"Too many intervals: {len(intervals)} (max 48)", None
    
    # Get device-specific power limit (if provided) or use default
    max_power_kw = schedule.get("max_power_kw", 50.0)  # Default to 50 kW if not specified
    
    # Validate each interval
    for i, interval in enumerate(intervals):
        if "start_time" not in interval or "power_kw" not in interval:
            return False, f"Interval {i} missing required fields", max_power_kw
        
        power_kw = interval["power_kw"]
        if not isinstance(power_kw, (int, float)):
            return False, f"Interval {i}: power_kw must be numeric", max_power_kw
        
        # Edge validation: Reject power_kw outside device-specific limits
        # This is a critical safety check to prevent hardware damage
        if abs(power_kw) > max_power_kw:
            return False, f"Interval {i}: power_kw {power_kw} exceeds device limit (±{max_power_kw} kW)", max_power_kw
        
        # Validate mode consistency if provided (mode must match power_kw sign)
        if "mode" in interval:
            mode = interval["mode"]
            expected_mode = "CHARGE" if power_kw > 0 else "DISCHARGE" if power_kw < 0 else "IDLE"
            if mode != expected_mode:
                return False, f"Interval {i}: mode '{mode}' does not match power_kw sign (expected '{expected_mode}')", max_power_kw
    
    return True, None, max_power_kw


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
    error_reason: Optional[str] = None,
    max_power_kw_applied: Optional[float] = None,
    device_id: Optional[str] = None,
    schedule_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Create an acknowledgement message to send back to cloud.
    
    Status values:
    - RECEIVED: Message received and validated (but not yet applied)
    - APPLIED: Schedule successfully applied to device
    - FAILED: Schedule could not be applied (see error_reason)
    
    Traceability: schedule_id links this ack to the original schedule,
    enabling cloud to track end-to-end request lifecycle.
    
    Args:
        schedule: Schedule dict (may be partial if parsing failed)
        status: Acknowledgement status
        error_reason: Error message if status is FAILED
        max_power_kw_applied: Power limit used for validation (for traceability)
        device_id: Device ID (fallback if schedule is incomplete)
        schedule_id: Schedule ID (fallback if schedule is incomplete)
    """
    # Handle cases where schedule is incomplete (e.g., JSON parsing failure)
    ack_schedule_id = schedule_id or schedule.get("schedule_id", "unknown")
    ack_device_id = device_id or schedule.get("device_id", "unknown")
    
    ack = {
        "schedule_id": ack_schedule_id,
        "device_id": ack_device_id,
        "status": status,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    
    if status == "FAILED" and error_reason:
        ack["error_reason"] = error_reason
    
    # Include max_power_kw_applied for traceability (helps cloud understand validation context)
    if max_power_kw_applied is not None:
        ack["max_power_kw_applied"] = max_power_kw_applied
    
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
    
    Note: Even if JSON parsing fails or schedule_id is missing, we send a generic
    device-level failed ack for monitoring. This ensures cloud has visibility into
    all message processing attempts, not just successful ones.
    """
    schedule = {}
    try:
        schedule = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # Invalid JSON - send generic device-level failed ack for monitoring
        # This ensures cloud has visibility even when schedule_id is unknown
        error_msg = f"Failed to parse message: {str(e)}"
        print(f"[DEVICE] ERROR: {error_msg}")
        # In production, use structured logging:
        # logger.error("schedule_parse_failed", error=str(e), device_id=device_id, topic=topic)
        
        ack = create_acknowledgement(
            schedule={},
            status="FAILED",
            error_reason=error_msg,
            device_id=device_id,
            schedule_id="unknown"
        )
        publish_acknowledgement(ack)
        return
    
    # In production, use structured logging:
    # logger.info("schedule_received", schedule_id=schedule.get('schedule_id'), device_id=device_id)
    print(f"[DEVICE] Received schedule on topic: {topic}")
    print(f"[DEVICE] Schedule ID: {schedule.get('schedule_id', 'unknown')}")
    
    # Validate schedule
    is_valid, error_reason, max_power_kw_applied = validate_schedule(schedule)
    
    if not is_valid:
        print(f"[DEVICE] Validation failed: {error_reason}")
        # In production, use structured logging:
        # logger.warning("schedule_validation_failed", error=error_reason, schedule_id=schedule.get('schedule_id'))
        
        ack = create_acknowledgement(
            schedule,
            "FAILED",
            error_reason,
            max_power_kw_applied=max_power_kw_applied
        )
        publish_acknowledgement(ack)
        return
    
    # Optional: Send RECEIVED ack for early confirmation
    # ack_received = create_acknowledgement(schedule, "RECEIVED", max_power_kw_applied=max_power_kw_applied)
    # publish_acknowledgement(ack_received)
    
    # Apply schedule to hardware
    try:
        success = apply_schedule(schedule)
        
        if success:
            ack = create_acknowledgement(
                schedule,
                "APPLIED",
                max_power_kw_applied=max_power_kw_applied
            )
            # In production, use structured logging:
            # logger.info("schedule_applied", schedule_id=schedule['schedule_id'], max_power_kw=max_power_kw_applied)
        else:
            ack = create_acknowledgement(
                schedule,
                "FAILED",
                "Hardware application failed",
                max_power_kw_applied=max_power_kw_applied
            )
        
        publish_acknowledgement(ack)
        
    except Exception as e:
        # Hardware error - send FAILED ack with error context
        error_msg = f"Hardware error: {str(e)}"
        print(f"[DEVICE] ERROR applying schedule: {e}")
        # In production, use structured logging:
        # logger.error("schedule_apply_error", error=str(e), schedule_id=schedule.get('schedule_id'))
        
        ack = create_acknowledgement(
            schedule,
            "FAILED",
            error_msg,
            max_power_kw_applied=max_power_kw_applied
        )
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
    
    Edge scaling note:
    - For single-device deployments, one topic per device is optimal
    - For multi-device edge gateways managing multiple devices, consider:
      * Topic filtering (wildcard subscriptions like devices/+/schedule)
      * MQTT shared subscriptions for load balancing across gateway instances
      * Gateway-level aggregation before forwarding to individual devices
    
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

