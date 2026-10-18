# CommerceETLAV Generator

[← Back to CommerceETLAV Main README](https://github.com/orbartal/commerce-etlav-manager/blob/main/README.md)

## Overview

**CommerceETLAV Generator** (`commerce-etlav-generator`) is a microservice in project: [CommerceETLAV](https://github.com/orbartal/commerce-etlav-manager). \
CommerceETLAV demonstrates Big Data, ETL, streaming, analytics, and AI/ML using microservice architecture. \
Generator simulates commerce activity and publishes predefined event scenarios to Kafka. \
Ingestor consumes these events and persists them to MinIO for downstream processing and analysis. \
Generator is designed for reproducible testing, simple operation, and future extensibility.

## Table of Contents

- [1. Context and Goals](#1-context-and-goals)
- [2. Architecture](#2-architecture)
- [3. Scenario and Event Model](#3-scenario-and-event-model)
- [4. PostgreSQL Storage](#4-postgresql-storage)
- [5. Python Initialization](#5-python-initialization)
- [6. REST API](#6-rest-api)
- [7. Kafka Publication](#7-kafka-publication)
- [8. Errors and Tracing](#8-errors-and-tracing)
- [9. Configuration](#9-configuration)
- [10. Testing and Cleanup](#10-testing-and-cleanup)
- [11. Development and Deployment](#11-development-and-deployment)
- [12. Future Extensions and References](#12-future-extensions-and-references)

---

## 1. Context and Goals

Generator belongs to the CommerceETLAV event-streaming flow:

```text
Generator → Kafka → Ingestor → MinIO
```

The event-streaming flow generates, transports, and persists commerce events for other services to analyze. \
In production, events would originate from real commerce systems. \
For the MVP, Generator simulates both the retailer and the gateway receiving retailer events.

### Commerce etlav Architecture

![Commerce ETLAV Architecture](https://raw.githubusercontent.com/orbartal/commerce-etlav-manager/refs/heads/main/docs/images/commerce_etlav_microservices_architecture_150kb.jpg)
See full context in [Manager README](https://github.com/orbartal/commerce-etlav-manager/blob/main/README.md) 
and more details in section [Architecture: Diagram 2.8](https://github.com/orbartal/commerce-etlav-manager/blob/main/docs/architecture.md#diagram-28---jpg-image). \
Generator microservice is inside group 'Data Generation and Ingestion' (left-middle of the entire diagram).

### Test Data Generation and Ingestion
The Tester initiates each test, validates the initial and final states, and resets the data. \
The Tester uses the Generator to create and publish data to Kafka. \
The Ingestor consumes data from Kafka and writes it to MinIO. \
The Tester validates the data in MinIO based on the scenario triggered by the Generator. \
The Tester uses the Cleaner microservice to delete all data before and after each test. \
If a test fails, the Tester preserves the data so the Developer can investigate the failure.

```text
Tester --> Cleaner (delete data from three services)
  ↓          ↓           ↓             ↓
Generator --> Kafka --> Ingestor --> MinIO
```

### Goals

- Generate realistic commerce event scenarios.
- Publish events to Kafka at a configurable rate.
- Support deterministic and reproducible testing.
- Allow developers to define scenarios without changing Java code.
- Provide REST APIs for scenario discovery, event lookup, and publication.
- Keep the MVP simple while allowing future extensions.

Generator does not implement commerce analytics, event ingestion, or persistent storage of processed events.

## 2. Generator Architecture

Generator contains two independent components.

### Java Application

A Java 27, Spring Boot 4, and Maven 3 microservice. \
It exposes REST APIs, manages scenario metadata, reads predefined events from PostgreSQL, and publishes them to Kafka. \
The application is stateless with respect to test execution. It does not maintain a persistent task history.

### Python Initialization

Standalone Python scripts generate scenario events and populate PostgreSQL before testing. \
Python initialization is executed manually by a developer. Java does not execute Python scripts in the MVP.

### Data Flow

```text
Scenario Configuration
        |
        v
Scenario JSON + Python Script
        |
        v
Python Manager + Shared Writer
        |
        v
    PostgreSQL
        |
        v
Java Generator REST API
        |
        v
      Kafka
        |
        v
     Ingestor
        |
        v
      MinIO
```

PostgreSQL is persistent scenario storage. Kafka and MinIO belong to the event-processing pipeline.

## 3. Scenario and Event Model

### Scenario Definition

A scenario defines a predefined, reproducible sequence of commerce events.

Each scenario has:

- An immutable UUID.
- A unique name.
- A human-readable description.
- One predefined dataset UUID.
- A predefined event sequence.
- A dedicated Python generation script.

The scenario JSON file is the authoritative source for scenario metadata. \
In the MVP, scenarios are registered before application startup. \
They cannot be created, modified, or deleted through REST APIs.

### Scenario Registration

Spring Boot configuration contains the authoritative list of registered scenario UUIDs. \
Each registered UUID must have a corresponding scenario JSON file and Python script. \
Generator validates these relationships at startup. \
Missing or invalid required files cause startup failure. \
Unregistered files and scripts are ignored.

### Event Model

Generated events follow the shared CommerceETLAV Business Model and Event Model.

The MVP supports:

1. `SESSION_START`
2. `ORDER_CREATED`
3. `ORDER_ITEM_ADDED`
4. `ORDER_SUBMITTED`
5. `PURCHASE`
6. `DELIVERED`
7. `SESSION_END`

All entity identifiers use immutable UUIDs.

Each scenario has one fixed `datasetId`. Events contain their original UUIDs, business data, timestamps, and sequence numbers. \
Event content and ordering remain unchanged across repeated publications. \
The original event data is deterministic. Kafka metadata and downstream processing metadata may differ between executions.

### Retail Clients

Retail clients are predefined in configuration and have immutable UUIDs. \
The MVP does not support dynamic client registration, authentication, or authorization.

## 4. PostgreSQL Storage

Generator uses PostgreSQL to store scenario metadata and original events. \
PostgreSQL data persists between tests and is never deleted by Cleaner.

### Scenario Table

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `name` | VARCHAR | Unique scenario name |
| `description` | TEXT | Human-readable scenario description |
| `dataset_id` | UUID | Unique dataset identifier |
| `is_ready` | BOOLEAN | Indicates successful event initialization |
| `event_count` | BIGINT | Number of initialized events |

Scenario UUID and name must be unique. A scenario is available for publication only when `is_ready = true`.

### Event Table

| Column | Type | Description |
|---|---|---|
| `id` | BIGINT | Generated numeric primary key |
| `event_uuid` | UUID | Unique event identifier |
| `event_index` | BIGINT | Zero-based event position within the scenario |
| `scenario_id` | UUID | Foreign key to Scenario |
| `event_body` | JSONB | Complete original event JSON |

Required constraints:

- Unique `event_uuid`.
- Unique `(scenario_id, event_index)`.
- Foreign key from `event.scenario_id` to `scenario.id`.

The database `event_index` starts at zero. The event's `eventSequence` starts at one.

### Storage Abstraction

Business logic must not depend directly on PostgreSQL or Hibernate repositories.

A dedicated storage interface exposes only the operations Generator requires:

- Retrieve a scenario.
- List scenarios with pagination.
- List ordered event UUIDs with pagination.
- Retrieve an event by scenario UUID and event UUID.
- Stream all events belonging to a scenario in order.
- Retrieve the scenario event count.

The PostgreSQL implementation is hidden behind this interface. \
This allows future replacement of DB without changing Generator business logic. \
Alternative DBs: file storage, another SQL database, or a document database

### Database Schema Management

Database tables are created by a separate initialization script. \
Hibernate validates the existing schema but does not create or modify tables. \
Database migration tools such as Flyway or Liquibase may be introduced later.

## 5. Python Initialization

Python is responsible for generating event data before Java publishes it. \
Each scenario has its own Python generator script. \
Shared Python components provide reusable generation and persistence functionality.

### Components

**EventsGenerator:** A scenario-specific Python iterator that yields one event at a time. \
**EventsWriter:** A shared component that writes generated events into PostgreSQL in batches of 1,000. \
**EventsManager:** A shared script that coordinates scenario loading, generation, persistence, and readiness validation. \
Scenario-specific scripts should contain only the logic unique to their scenarios.

### Initialization Process

1. Read the scenario UUID and locate its metadata and generator script.
2. Verify that the scenario exists in PostgreSQL.
3. Reject initialization if `is_ready = true`.
4. Keep `is_ready = false` and reset `event_count`.
5. Delete existing events belonging to the scenario.
6. Generate events using the scenario-specific Python iterator.
7. Insert events in batches of 1,000, committing each batch.
8. Validate the final event count and event-index continuity.
9. Update `event_count` with the actual count.
10. Set `is_ready = true` as the final successful database operation.

If initialization fails, the script exits with an error and leaves `is_ready = false`. \
Previously committed batches remain available for investigation. \
A subsequent initialization attempt deletes those incomplete events and restarts from event zero.

### Scenario Immutability

The manager refuses to initialize a scenario that is already ready. \
The MVP does not provide an API to regenerate or overwrite a ready scenario. \
A developer may manually modify PostgreSQL to permit regene|ation, but this is not recommended.

### Initialization Safety

Python initialization should normally finish before Generator begins publishing scenarios. \
Java checks scenario readiness before publication. \
The MVP does not implement distributed locking between Python and Java. \
Developers must not modify scenario event data while that scenario is being published.

## 6. REST API

Generator exposes versioned REST APIs. \
Every REST request receives a new random UUID, `requestId`, for tracing. \
The request UUID appears in application logs and API responses.

### API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/scenarios` | List registered scenarios |
| GET | `/api/v1/scenarios/{scenarioId}` | Get scenario metadata |
| GET | `/api/v1/scenarios/{scenarioId}/events` | List ordered event UUIDs |
| GET | `/api/v1/scenarios/{scenarioId}/events/{eventId}` | Get one complete original event |
| POST | `/api/v1/scenarios/{scenarioId}/publish` | Publish all scenario events to Kafka |

### Pagination

List endpoints support standard zero-based page-number pagination.

| Parameter | Default | Description |
|---|---|---|
| `page` | `0` | Zero-based page number |
| `size` | `10` | Maximum items returned per page |

Pagination must preserve deterministic ordering. \
Scenario lists are ordered consistently. Event lists are ordered by `event_index`. \
Paginated responses include the returned items and pagination metadata, including total item count.

### List Scenarios

`GET /api/v1/scenarios`

Each scenario item contains:

- `id`
- `name`
- `isReady`
- `eventCount`

### Get Scenario

`GET /api/v1/scenarios/{scenarioId}`

Returns all scenario metadata:

- `id`
- `name`
- `description`
- `datasetId`
- `isReady`
- `eventCount`

### List Events

`GET /api/v1/scenarios/{scenarioId}/events`

Returns event UUIDs in scenario order, together with their zero-based event indexes. \
Supports pagination.

### Get Event

`GET /api/v1/scenarios/{scenarioId}/events/{eventId}`

Returns the complete original event JSON stored in PostgreSQL. \
The scenario UUID identifies the dataset to search.

### Publish Scenario

`POST /api/v1/scenarios/{scenarioId}/publish`

Publishes every event belonging to the scenario. \
The request optionally specifies `eventsPerSecond`. \
The default publication rate is one event per second. The value must remain within configured minimum and maximum limits. \
The MVP does not support publishing only part of a scenario.

### Publication Response

Publication is synchronous.

A successful response contains:

| Field | Description |
|---|---|
| `requestId` | UUID identifying this REST request |
| `scenarioId` | UUID of the published scenario |
| `status` | `COMPLETED` |
| `publishedEventCount` | Number of successfully acknowledged events |
| `comment` | Additional information |

`COMPLETED` means Generator finished publishing to Kafka. \
It does not mean Ingestor processed the events or MinIO persisted them. \

### Publication Failure

If an event cannot be published, Generator stops immediately and returns an error.

The failure response includes:

- Request UUID.
- `FAILED` status.
- Number of successfully acknowledged events.
- UUID of the failed event.
- Zero-based index of the failed event.
- Standard error details.

Events already acknowledged by Kafka are not rolled back. \
The MVP does not implement automatic retries, cancellation, or recovery.

## 7. Kafka Publication

Generator publishes original scenario events to Kafka. \
Events are read from PostgreSQL in ascending `event_index` order.

### Publication Rules

- Publish one event at a time.
- Wait for Kafka acknowledgment before publishing the next event.
- Preserve scenario event ordering.
- Use `datasetId` as the Kafka message key.
- Apply the requested events-per-second rate.
- Stop immediately when publication fails.

The original event JSON must not be modified during publication.

### Rate Limiting

Publication rate is independent of scenario content. \
The same scenario can be published at different rates without changing its events.

Global configuration defines:

- Default publication rate: 1 event/second.
- Minimum supported rate.
- Maximum supported rate.

The MVP prioritizes predictable behavior over maximum throughput.

### Concurrency

Only one publication task may run at a time, regardless of scenario. \
A second publication request receives HTTP `409 Conflict`. \
The application does not queue publication requests.

### Failure Handling

Generator reports the first failed event and the number of acknowledged events. \
Kafka acknowledgment failures may be ambiguous. \
The acknowledged count does not necessarily equal the number of events actually stored by Kafka. \
The MVP does not provide transactional all-or-nothing publication.

## 8. Errors and Tracing

Generator uses a consistent exception architecture.

### Exception Model

`GeneratorException` extends `RuntimeException`.

Every instance contains:

- `message`: Human-readable error description.
- `type`: An `ErrorType` identifying the error.
- `instance`: A random UUID uniquely identifying this exception occurrence.
- Optional original exception cause.

`HttpGeneratorException` extends `GeneratorException` and additionally contains an HTTP status.

### Error Types

`ErrorType` is an interface implemented only by Java enums.

Each enum value identifies a specific exception creation location. \
The convention is to use each error enum value at one exception factory location in code. \
In order to make it easier to locate the source of an error.

### Exception Factory

`GeneratorExceptionFactory` is a marker interface with many implementations ExceptionFactories (stateless classes). \
Each ExceptionFactory provides methods that construct exceptions with appropriate messages, types, causes, and HTTP statuses. \
Other components use this factory instead of constructing application exceptions directly.

### REST Error Handling

A centralized REST exception handler converts exceptions into consistent HTTP error responses.

An error response contains:

- `requestId`
- `status`
- `error.type`
- `error.instance`
- `error.message`

Publication failures additionally include the failed event UUID, event index, and acknowledged event count when available. \
The `requestId` identifies the API request. The exception `instance` identifies the individual error occurrence. \
Both identifiers appear in logs. Internal stack traces and implementation details are not exposed to API callers.

### Python Errors

Python scripts are standalone developer tools. \
They may use ordinary Python exceptions and readable error messages. \
The Java exception architecture does not apply to Python.

## 9. Configuration

Generator uses Spring Boot configuration and environment variables.

### Runtime Properties

Configuration includes:

- PostgreSQL connection settings.
- Kafka bootstrap servers.
- Kafka output topic.
- REST server port.
- Registered scenario UUID list.
- Scenario metadata directory.
- Python scripts directory.
- Retail client configuration.
- Default, minimum, and maximum publication rates.
- Logging settings.

### External Files

Scenario metadata and Python scripts use configurable directories. \
Development defaults may point to Spring Boot resources. \
Docker deployments use externally mounted directories. \
Thus, developers can add scenarios without rebuilding the Java application or Docker image. 

### File Naming

Scenario metadata:

`scenario_{UUID}.json`

Scenario generator script:

`event_generator_{UUID}.py`

Retail client configuration:

`clients.json`

The registered scenario UUID must match the UUID in its metadata and corresponding script filename.

### Startup Validation

Generator validates registered scenarios at startup.

It must verify:

- Registered UUIDs are valid and unique.
- Every registered scenario has a metadata JSON file.
- Every registered scenario has a matching Python script.
- Scenario metadata is valid and its UUID matches the configuration.
- Required retail client configuration is valid.
- The PostgreSQL schema is valid.

Missing or invalid required configuration causes startup failure. \
Unregistered scenario files and scripts are ignored. \
Missing generated event data does not prevent startup. Such scenarios remain unavailable for publication until initialized.

## 10. Testing and Cleanup

Generator is validated through the external `commerce-etlav-tester` microservice.

The initial end-to-end pipeline is:

```text
Tester
  |
  +--> Cleaner
  |
  +--> Generator --> Kafka --> Ingestor --> MinIO
  |
  +--> Generator REST API
  |
  +--> MinIO REST API
```

### Test Execution

Tester coordinates the following operations:

1. Explicitly invoke Cleaner to remove transient pipeline data.
2. Use Cleaner validation APIs to confirm the required services are clean.
3. Select a ready Generator scenario.
4. Trigger synchronous scenario publication.
5. Wait for the expected events to become available in MinIO.
6. Retrieve expected event information using Generator REST APIs.
7. Compare persisted events against the original Generator events.
8. Report the test result.

### Validation

Tests verify:

- Expected event count.
- Event UUIDs and ordering.
- Required event fields.
- Event data consistency.
- Business relationships.
- Successful persistence to MinIO.

The comparison focuses on the original event data. \
Kafka metadata and downstream processing metadata may differ between runs.

### Cleaner Responsibilities

`commerce-etlav-cleaner` is a separate microservice. \
It provides cleanup and validation APIs for individual pipeline services and data types, including Kafka, Ingestor, and MinIO. \
Cleaner never deletes Generator's PostgreSQL scenarios or events. \
Generator's source data persists across tests.

### Failure Investigation

If a test fails, Tester stops and reports the failure. \
**Cleanup must not run automatically after failure.** \
The developer must be able to inspect Kafka, Ingestor, MinIO, and other relevant state before explicitly requesting cleanup. \
The MVP supports one test at a time.

## 11. Development and Deployment

### Technology Stack

| Component | Technology |
|---|---|
| Java runtime | Java 27 |
| Java framework | Spring Boot 4 |
| Build system | Maven 3 |
| Scenario generation | Python |
| Database | PostgreSQL |
| ORM and validation | Hibernate |
| Event transport | Apache Kafka |
| API | REST |
| Containerization | Docker |
| Local orchestration | Docker Compose |

### Development Principles

- Keep the MVP small and understandable.
- Prefer standard libraries and established frameworks.
- Separate business logic from persistence and infrastructure.
- Keep scenario-specific logic in Python scripts.
- Reuse shared Python generation and writing components.
- Avoid unnecessary frameworks and abstractions.
- Favor reproducibility and clear error reporting over performance optimization.

### Local Development

The initial implementation should run locally before Docker Compose integration. \
Database schema initialization is performed separately from the Java application. \
Python initialization populates scenario data before publication. \
The Java application validates the database schema and registered scenarios at startup.

### Docker Compose

The initial end-to-end environment will include:

- Generator
- PostgreSQL
- Kafka
- Ingestor
- MinIO
- Tester
- Cleaner

Scenario metadata and Python script directories are mounted into Generator. \
PostgreSQL uses persistent storage that remain after docker restart. \ 
The Java JAR and Docker image do not contain large generated event datasets. \
Docker configuration and startup instructions will be added during implementation.

## 12. Future Extensions and References

### Future Extensions

Possible future capabilities include:

- Asynchronous publication tasks with task UUIDs.
- Task status, progress, results, and logs.
- Task cancellation and pause/resume.
- Polling APIs and push notifications using WebSocket or pub/sub.
- Kafka retries and failure recovery.
- Higher-throughput asynchronous publication.
- Partial scenario publication.
- Multiple datasets per scenario.
- Dynamic scenario registration.
- Java-triggered Python initialization.
- Remote scenario and event storage.
- Multiple concurrent tests with tenant isolation.
- Invalid, duplicate, delayed, or out-of-order events.
- Authentication, authorization, and production security.
- Shared Java exception libraries.
- Database migrations using Flyway or Liquibase.

These capabilities are outside the MVP unless explicitly introduced in a later iteration.

### Related Documents

- [CommerceETLAV Main README](https://github.com/orbartal/commerce-etlav-manager/blob/main/README.md)
- [Architecture](https://github.com/orbartal/commerce-etlav-manager/blob/main/docs/architecture.md)
- [Business Model](https://github.com/orbartal/commerce-etlav-manager/blob/main/docs/business.md)
- [Event Model](https://github.com/orbartal/commerce-etlav-manager/blob/main/docs/events.md)

### Related Microservices

- `commerce-etlav-ingestor` — Consumes Kafka events and persists raw data to MinIO.
- `commerce-etlav-tester` — Coordinates and validates end-to-end tests.
- `commerce-etlav-cleaner` — Cleans and validates transient pipeline state.