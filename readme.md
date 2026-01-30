# Remote Battery Control System

## Purpose

This repository is a companion implementation reference for a system design exercise.. It demonstrates the message schemas, component boundaries, and communication flows for a distributed battery control system that manages battery management devices via MQTT.

**Note**: This code is illustrative and exists to show implementation intent and support architectural discussions.

## System Overview

A cloud service publishes daily battery schedules to edge devices (Raspberry Pi) via MQTT. Each device subscribes to its own topic, validates and applies the schedule locally, then sends an acknowledgement back to the cloud. The system is designed to scale to 10,000+ devices while maintaining observability, traceability, and extensibility.

- **Cloud Service**: Publishes daily battery schedules to devices
- **Edge Devices**: Subscribe to schedules, apply them locally, and send acknowledgements
- **Scale Target**: 10,000+ devices
- **Design Goals**: Observability, traceability, and future extensibility

## Repository Structure

```
.
├── README.md                    # This file
├── architecture/                # Architecture diagrams (placeholders)
│   ├── system-overview.png
│   └── message-flow.png
├── schemas/                     # JSON message schemas
│   ├── schedule.schema.json     # Battery schedule message format
│   ├── ack.schema.json         # Device acknowledgement format
│   └── execution_result.schema.json  # Per-interval execution result format
├── cloud/                       # Cloud-side examples
│   └── publish_schedule.py     # Example schedule publisher
├── device/                      # Device-side examples
│   └── mqtt_client.py          # Example MQTT subscriber
└── demo_end_to_end.py          # End-to-end workflow demonstration
```

## How It Works

### Message Flow

1. **Cloud publishes schedule**: The cloud service publishes a daily battery schedule to a device-specific MQTT topic
2. **Device subscribes**: Each device subscribes to its own topic (e.g., `devices/{device_id}/schedule`)
3. **Device validates**: The device validates the schedule schema and version
4. **Device applies**: If valid, the device applies the schedule locally and stores it for execution
5. **Device acknowledges**: The device publishes an acknowledgement message back to the cloud
6. **Device executes**: Device executes intervals independently based on system time
7. **Device reports**: Device publishes execution results every 30 minutes at interval end
8. **Cloud monitors**: Cloud receives execution results for observability and optimization

### Component Responsibilities

**Cloud Service:**
- Generates daily schedules for each device
- Publishes schedules with idempotency guarantees (via `schedule_id`)
- Monitors acknowledgement messages for observability
- Handles retries and error scenarios

**Edge Device:**
- Subscribes to device-specific MQTT topics
- Validates incoming schedules (schema + version)
- Applies schedules safely (rejects unknown versions)
- Stores schedules locally for independent execution
- Publishes acknowledgements for traceability
- Executes intervals independently based on system time
- Publishes execution results every 30 minutes
- Handles reconnection and retry logic

## Design Principles

### Observability

- **Structured logging**: Timestamps and trace IDs in all messages
- **Acknowledgement tracking**: Cloud correlates schedules with device responses
- **Status reporting**: RECEIVED, APPLIED, or FAILED status per device
- **Per-interval execution results**: Devices report execution status every 30 minutes
- **Actual vs scheduled comparison**: Execution results include actual_rate_kw for optimization
- **Error context**: Failed operations include error reasons

### Scalability

- **Topic partitioning**: Device-specific topics enable horizontal scaling
- **QoS levels**: Appropriate MQTT QoS for reliable delivery
- **Idempotency**: Schedule IDs enable safe retries and duplicate handling
- **Stateless design**: Components remain stateless where possible

### Future Extensibility

- **Versioning**: Schedule messages include version fields for protocol evolution
- **Schema evolution**: JSON schemas support backward-compatible extensions (e.g., optional metadata fields for renewable energy tags or market signals)
- **Protocol flexibility**: JSON can be replaced with Protobuf without changing component boundaries
- **Clear boundaries**: Separation enables independent evolution of cloud and device logic

## Usage

### Viewing Schemas

The JSON schemas in `schemas/` define the contract between cloud and device components. They include detailed comments explaining field purposes and design decisions.

### Running Examples

The Python examples in `cloud/` and `device/` demonstrate:
- Topic naming conventions
- Message serialization
- Basic MQTT patterns
- Error handling approaches

**End-to-End Demo:**
Run `python demo_end_to_end.py` to see the complete workflow:
1. Cloud generates and publishes schedule
2. Device receives and validates schedule
3. Device applies schedule
4. Device sends acknowledgement
5. Cloud receives acknowledgement

This demonstrates the full message flow in a single script.

### Message Format Notes

**Power and Mode Fields:**
- `rate_kw` is the **source of truth** for battery operation (positive=charge, negative=discharge, zero=idle)
- Matches PDF field name `rate_kw` (was `power_kw` in earlier version)
- Optional `mode` field exists for dashboard visualization and traceability only
- Devices derive mode from `rate_kw` sign; explicit `mode` is ignored during validation

**Device-Specific Limits:**
- Optional `max_power_kw` field enables device-specific edge validation
- Devices reject schedules where `abs(power_kw) > max_power_kw` (or default limit if not specified)
- Supports different device models with varying power capabilities

## What This Repository Is Not

- ❌ Production-ready codebase
- ❌ Complete implementation
- ❌ Deployment guide

## What This Repository Is

- ✅ Architectural reference
- ✅ Schema definitions
- ✅ Component boundary illustration
- ✅ Communication flow example

## Related Documentation

This repository complements a separate architecture document that covers:
- System topology
- Deployment strategies
- Monitoring and alerting
- Failure modes and recovery
- Performance characteristics

---

**Note**: Architecture diagrams referenced in `architecture/` are placeholders. In a real design document, these would contain detailed system diagrams showing component interactions, data flows, and deployment topologies.
