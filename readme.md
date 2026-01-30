# Remote Battery Control System

## Purpose

This repository serves as a **companion implementation reference** for a system design exercise. It demonstrates the message schemas, component boundaries, and communication flows for a distributed battery control system that manages Allye MAX300 devices via MQTT.

**This code is illustrative, not production-ready.** It exists to show implementation intent and support architectural discussions, not to be deployed as-is.

## System Overview

The system enables cloud-based control of edge battery devices:

- **Cloud Service**: Publishes daily battery schedules to devices
- **Edge Devices** (Raspberry Pi): Subscribe to schedules, apply them locally, and send acknowledgements
- **Scale Target**: Designed to support 10,000+ devices
- **Core Design Goals**: Observability, traceability, and future extensibility

## Repository Structure

```
.
├── README.md                    # This file
├── architecture/                # Architecture diagrams (placeholders)
│   ├── system-overview.png
│   └── message-flow.png
├── schemas/                     # JSON message schemas
│   ├── schedule.schema.json     # Battery schedule message format
│   └── ack.schema.json         # Device acknowledgement format
├── cloud/                       # Cloud-side examples
│   └── publish_schedule.py     # Example schedule publisher
└── device/                      # Device-side examples
    └── mqtt_client.py          # Example MQTT subscriber
```

## How It Works

### Message Flow

1. **Cloud publishes schedule**: The cloud service publishes a daily battery schedule to a device-specific MQTT topic
2. **Device subscribes**: Each device subscribes to its own topic (e.g., `devices/{device_id}/schedule`)
3. **Device validates**: The device validates the schedule schema and version
4. **Device applies**: If valid, the device applies the schedule locally
5. **Device acknowledges**: The device publishes an acknowledgement message back to the cloud

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
- Publishes acknowledgements for traceability
- Handles reconnection and retry logic

## Design Principles

### Observability

- **Structured logging**: All messages include timestamps and trace IDs
- **Acknowledgement tracking**: Cloud can correlate schedules with device responses
- **Status reporting**: Devices report RECEIVED, APPLIED, or FAILED status
- **Error context**: Failed operations include error reasons for debugging

### Scalability

- **Topic partitioning**: Device-specific topics enable horizontal scaling
- **QoS levels**: Appropriate MQTT QoS ensures message delivery without overwhelming the broker
- **Idempotency**: Schedule IDs allow safe retries and duplicate handling
- **Stateless design**: Cloud and device components remain stateless where possible

### Future Extensibility

- **Versioning**: Schedule messages include version fields to support protocol evolution
- **Schema evolution**: JSON schemas can be extended with backward-compatible fields
- **Protocol flexibility**: Current JSON implementation can be replaced with Protobuf or other formats without changing component boundaries
- **Clear boundaries**: Separation between cloud and device logic enables independent evolution

## Usage

### Viewing Schemas

The JSON schemas in `schemas/` define the contract between cloud and device components. They include detailed comments explaining field purposes and design decisions.

### Running Examples

The Python examples in `cloud/` and `device/` are illustrative only. They demonstrate:
- Topic naming conventions
- Message serialization
- Basic MQTT patterns
- Error handling approaches

**Note**: These scripts are not meant to be executed in production. They lack proper error handling, configuration management, and security measures required for real deployments.

## What This Repository Is Not

- ❌ A production-ready codebase
- ❌ A complete implementation
- ❌ A deployment guide
- ❌ A testing framework

## What This Repository Is

- ✅ An architectural reference
- ✅ A schema definition
- ✅ An illustration of component boundaries
- ✅ A communication flow example
- ✅ A design discussion tool

## Related Documentation

This repository complements a separate architecture document that covers:
- System topology
- Deployment strategies
- Monitoring and alerting
- Failure modes and recovery
- Performance characteristics

---

**Note**: Architecture diagrams referenced in `architecture/` are placeholders. In a real design document, these would contain detailed system diagrams showing component interactions, data flows, and deployment topologies.
