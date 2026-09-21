# Temporal + Spring Boot Example

An order processing workflow demonstrating Temporal configuration and coding patterns with Spring Boot. The workflow runs four steps in sequence: validate → payment → fulfill → notify.

## Prerequisites

- Java 17 (project is pinned to `~/.sdkman/candidates/java/17.0.1-tem` via `gradle.properties`)
- One of: Temporal CLI **or** Docker + Docker Desktop

---

## Starting Temporal

### Option A — Temporal CLI (simplest)

Install once:

```bash
brew install temporal
```

Start the dev server:

```bash
temporal server start-dev
```

- gRPC server: `localhost:7233`
- Web UI: http://localhost:8233

### Option B — Docker Compose (already in the project)

Make sure Docker Desktop is running, then:

```bash
docker-compose up -d
```

Same ports — gRPC on 7233, Web UI at http://localhost:8233.

To stop:

```bash
docker-compose down
```

---

## Running the Application

With Temporal already running:

```bash
./gradlew bootRun
```

Watch for this line in the logs — it confirms the worker connected:

```
Temporal worker started on task queue 'order-task-queue'
```

The Spring Boot app listens on `http://localhost:8080`.

---

## Submitting an Order

```bash
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "order-001",
    "customerId": "cust-42",
    "amount": 99.99,
    "items": ["widget-A", "widget-B"]
  }' | jq .
```

Expected response (202 Accepted):

```json
{
  "workflowId": "order-order-001",
  "runId": "<uuid>",
  "status": "STARTED"
}
```

Open the Temporal Web UI at http://localhost:8233 to watch the workflow run through its four activities.

### Simulating a payment failure

Set the flag in `application.yml`:

```yaml
temporal:
  activities:
    simulate-payment-failure: true
```

Restart the app and submit an order — the payment activity will fail twice (retries), then succeed on the third attempt. You can watch the retry attempts in the Web UI timeline.

---

## Running the Tests

Tests use `TestWorkflowEnvironment` — an in-process Temporal server with simulated time. **No real Temporal server or Docker is needed.**

```bash
./gradlew test
```

### Viewing test output

```bash
./gradlew test --info
```

HTML report after the run:

```
build/reports/tests/test/index.html
```

---

## Key Patterns Demonstrated

**Retry options per activity** — each of the four activities has distinct `RetryOptions` in `OrderWorkflowImpl.java` reflecting different risk profiles (e.g., payment gets 5 attempts; fulfillment gets 3).

**Heartbeat for long-running activities** — `fulfillOrder` calls `Activity.getExecutionContext().heartbeat(step)` inside its processing loop. On restart after a `heartbeatTimeout`, it resumes from `getHeartbeatDetails()` rather than starting over.

**Idempotent workflow IDs** — the controller uses `workflowId = "order-" + orderId`, so re-submitting the same order ID returns the existing execution rather than starting a duplicate.

**Replay-safe logging** — workflow code uses `Workflow.getLogger()`, not `LoggerFactory.getLogger()`, because workflow code can replay and `LoggerFactory` would emit duplicate log lines.

**TestWorkflowEnvironment** — tests control simulated time with `testEnv.sleep()`, so the timeout test completes in milliseconds rather than waiting for a real 5-second deadline.
