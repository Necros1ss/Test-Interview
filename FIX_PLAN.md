# FIX PLAN — Techlab Dev Interview 2026

> Goal: nâng mức độ đáp ứng của source hiện tại từ khoảng **67% → 85–90%** so với yêu cầu bài test.
>
> Scope: ưu tiên **correctness, reliability, message-driven architecture và proof**, không thêm feature không cần thiết.

---

## 0. Baseline

### Current assessment

- Overall: ~67%
- RabbitMQ architecture: ~70%
- Async Order: ~90%
- Payment retry/DLQ: ~75–80%
- Inventory concurrency: ~70%
- Saga correctness: ~65%
- Database persistence: ~20%
- Testing: ~75%
- Benchmark: ~50%
- RabbitMQ vs Kafka/ActiveMQ proof: ~40%
- Production readiness: ~50%

### Baseline checklist

- [ ] Run `npm test`
- [ ] Record current test result
- [ ] Run `docker compose up --build`
- [ ] Verify all services start correctly
- [ ] Run current benchmark
- [ ] Record RPS, p50, p95, p99
- [ ] Create Git commit:
  - `chore: baseline before architecture fixes`

---

# P0 — MUST FIX

## 1. Fix RabbitMQ lifecycle

### Problem

Current services can initialize RabbitMQ/consumers as a module side effect. This can cause duplicate consumers during testing and makes lifecycle management difficult.

### Target architecture

```text
server.js
   |
   +--> connectRabbitMQ()
   |
   +--> declareTopology()
   |
   +--> startConsumers()
   |
   +--> app.listen()
```

### Tasks

- [ ] Separate RabbitMQ connection logic from application startup
- [ ] Separate publisher from consumer initialization
- [ ] Avoid automatically starting consumers when a module is imported
- [ ] Make service startup explicit
- [ ] Make test lifecycle explicit
- [ ] Add graceful shutdown
- [ ] Close RabbitMQ channel/connection on SIGTERM/SIGINT

### Expected result

- No duplicate consumers
- Tests can start/stop dependencies deterministically
- Kubernetes shutdown does not leave consumers hanging

---

## 2. Remove production in-memory RabbitMQ fallback

### Current problem

Do NOT silently fall back from RabbitMQ to an in-memory EventEmitter when RabbitMQ is unavailable.

```text
RabbitMQ unavailable
       |
       X
       |
In-memory broker
```

This can cause message loss and does not work correctly across multiple service processes/pods.

### Target behavior

```text
RabbitMQ unavailable
       |
       v
Reconnect
       |
       +--> success -> continue
       |
       +--> failure -> service unhealthy / fail startup
```

### Tasks

- [ ] Remove automatic fallback from production path
- [ ] Keep fake broker only for unit tests if needed
- [ ] Gate fake broker behind `NODE_ENV=test`
- [ ] Add RabbitMQ reconnect handling
- [ ] Recreate channel after reconnect
- [ ] Redeclare topology after reconnect
- [ ] Restart consumers after reconnect

---

## 3. Add PostgreSQL persistence

The exercise explicitly requires Order Service to persist order data to a database.

### Target

```text
Order Service
     |
     v
PostgreSQL
```

### Minimum schema

#### `orders`

```text
id
user_id
status
total_amount
created_at
updated_at
```

#### `order_items`

```text
id
order_id
product_id
quantity
price
```

Optional but recommended:

#### `order_status_history`

```text
id
order_id
old_status
new_status
reason
created_at
```

### Tasks

- [ ] Add PostgreSQL to Docker Compose
- [ ] Add database configuration through environment variables
- [ ] Create migrations/schema
- [ ] Replace in-memory `orders = new Map()`
- [ ] Create `orderRepository`
- [ ] Keep business logic inside `orderService`
- [ ] Keep HTTP handling inside controller
- [ ] Add DB indexes where appropriate
- [ ] Add transaction for order + order items creation

### Target layering

```text
Controller
    |
    v
Service
    |
    v
Repository
    |
    v
PostgreSQL
```

---

## 4. Fix Saga correctness

### Current problem

Payment and Inventory can currently run in parallel.

Potential failure:

```text
Order Created
     |
     +----> Payment SUCCESS
     |
     +----> Inventory FAILED
```

Result:

```text
Payment = SUCCESS
Order   = FAILED
Inventory = no stock
```

This creates an inconsistent business state.

### Recommended flow

Use Inventory reservation before Payment:

```text
Order Created
      |
      v
Inventory Reservation
      |
      +------------------+
      |                  |
   SUCCESS             FAILED
      |                  |
      v                  v
Inventory Reserved   Order FAILED
      |
      v
Payment
      |
      +------------------+
      |                  |
   SUCCESS             FAIL
      |                  |
      v                  v
Order PAID          Payment Retry
                         |
                    max retries
                         |
                         v
                    Order FAILED
```

### Tasks

- [ ] Change Order -> Inventory to first step
- [ ] Publish `inventory.reserved`
- [ ] Publish `inventory.failed`
- [ ] Start Payment only after inventory reservation succeeds
- [ ] Update Order to `PAID` only after payment success
- [ ] Update Order to `FAILED` on permanent payment failure
- [ ] Define valid order state transitions
- [ ] Reject invalid state transitions
- [ ] Document Saga state machine

### Important

Do NOT keep the current parallel Payment + Inventory flow unless implementing a complete compensation mechanism.

---

# P1 — IMPORTANT

## 5. Fix Inventory concurrency

The current in-memory lock is not safe across multiple Kubernetes pods.

### Problem

```text
Load Balancer
    |
    +--> Pod 1 -> Lock 1
    +--> Pod 2 -> Lock 2
    +--> Pod 3 -> Lock 3
```

Each process has its own lock.

### Recommended solution

Use an atomic database update:

```sql
UPDATE inventory
SET stock = stock - $1
WHERE product_id = $2
  AND stock >= $1;
```

Then:

```text
affectedRows = 1 -> reservation success
affectedRows = 0 -> out of stock
```

### Tasks

- [ ] Add inventory table
- [ ] Add atomic stock decrement
- [ ] Remove reliance on process-local locking
- [ ] Add transaction where reservation requires multiple DB operations
- [ ] Add inventory reservation table
- [ ] Add unique constraint for `(order_id, product_id)`
- [ ] Test with multiple concurrent requests
- [ ] Test against real PostgreSQL

### Expected result

For stock = 5 and 20 concurrent requests:

```text
5 successful reservations
15 rejected
stock = 0
```

No overselling.

---

## 6. Add idempotency

RabbitMQ consumers must tolerate duplicate messages.

### Payment

```text
payments
----------------
id
order_id UNIQUE
status
transaction_id
```

### Inventory

```text
inventory_reservations
----------------
id
order_id
product_id
quantity
status

UNIQUE(order_id, product_id)
```

### Optional event table

```text
processed_events
----------------
event_id UNIQUE
event_type
processed_at
```

### Consumer flow

```text
Receive event
     |
     v
Check event_id
     |
     +--> already processed -> ACK
     |
     +--> new event
              |
              v
          process
              |
              v
       record processed
              |
              v
             ACK
```

### Tasks

- [ ] Remove reliance on in-memory `Set`/`Map` for production idempotency
- [ ] Add DB constraints
- [ ] Make Payment idempotent
- [ ] Make Inventory reservation idempotent
- [ ] Make Order event handling idempotent
- [ ] Add duplicate-event integration tests

---

## 7. Harden Payment retry / DLQ

Keep the existing RabbitMQ DLX + TTL approach.

### Target

```text
payment.queue
      |
      v
Payment Consumer
      |
   failure
      |
      v
payment.dlx
      |
      v
payment.retry.queue
      |
      | TTL
      v
payment.queue
      |
      | max retry exceeded
      v
payment.parking.queue
```

### Tasks

- [ ] Keep retry count in message metadata
- [ ] Define max retry count = 3
- [ ] Verify retry count increments correctly
- [ ] Preserve correlation ID across retries
- [ ] Preserve original order/event identifiers
- [ ] Send permanently failed messages to parking queue
- [ ] Add real integration test for retry flow
- [ ] Add real integration test for parking queue

### Important

Do not only test constants such as queue/exchange names.

Test the actual message movement.

---

## 8. Consider Transactional Outbox

### Problem

Without an outbox:

```text
DB INSERT succeeds
       |
       X
RabbitMQ publish fails
```

The order exists but `order.created` is lost.

Or:

```text
RabbitMQ publish succeeds
       |
       X
DB transaction rolls back
```

The event exists without the order.

### Recommended architecture

```text
Order Service
      |
      v
PostgreSQL
+-------------------+
| orders            |
| outbox_events     |
+-------------------+
         |
         v
Outbox Publisher
         |
         v
RabbitMQ
```

### Tasks

- [ ] Add `outbox_events`
- [ ] Insert Order + Outbox Event in same DB transaction
- [ ] Create outbox publisher worker
- [ ] Publish unpublished events
- [ ] Mark events as published
- [ ] Make publisher idempotent
- [ ] Add failure/retry test

### Priority

Strongly recommended if time allows.

If time is limited, implement DB persistence + Saga + idempotency first.

---

# P1 — TESTING

## 9. Build real integration tests

Current tests passing is good, but several tests should verify behavior rather than constants/internal data structures.

### Test environment

```text
PostgreSQL
RabbitMQ
Services
```

### Happy path

```text
Create Order
    |
Inventory Reserved
    |
Payment Success
    |
Order PAID
```

Expected:

- [ ] Order = PAID
- [ ] Inventory decreased
- [ ] Payment = SUCCESS

---

### Inventory failure

```text
Create Order
    |
Inventory
    |
OUT_OF_STOCK
```

Expected:

- [ ] Order = FAILED
- [ ] Payment not started
- [ ] Stock unchanged

---

### Payment transient failure

```text
Inventory Reserved
      |
Payment FAIL
      |
Retry #1
      |
Retry #2
      |
Payment SUCCESS
      |
Order PAID
```

Expected:

- [ ] Payment eventually succeeds
- [ ] Order = PAID
- [ ] Inventory remains correctly reserved

---

### Payment permanent failure

```text
Payment FAIL
   |
Retry #1
   |
Retry #2
   |
Retry #3
   |
Parking Queue
```

Expected:

- [ ] Payment = FAILED
- [ ] Order = FAILED
- [ ] Message exists in parking queue

---

### Duplicate event

Send:

```text
payment.success
payment.success
payment.success
```

Expected:

- [ ] Payment processed exactly once
- [ ] Order state remains correct
- [ ] No duplicate side effect

---

### Concurrent inventory

```text
stock = 5
20 concurrent requests
```

Expected:

```text
5 success
15 failure
stock = 0
```

Run this against PostgreSQL, not only an in-memory Map.

---

# P2 — BENCHMARK

## 10. Redesign benchmark methodology

Current async benchmark mainly measures:

```text
POST /orders
    |
publish event
    |
202 response
```

while sync measures more of the downstream workflow.

Therefore do not claim the async system is globally 100x faster based only on HTTP latency.

### Measure separately

#### A. API acceptance latency

```text
POST /orders
      |
      v
202 Accepted
```

Measure:

- [ ] Throughput
- [ ] p50
- [ ] p95
- [ ] p99
- [ ] Error rate

#### B. End-to-end completion latency

```text
POST /orders
      |
      v
Order Created
      |
Inventory
      |
Payment
      |
Order PAID / FAILED
```

Measure:

- [ ] Time to completion
- [ ] Success rate
- [ ] Failure rate
- [ ] Retry count
- [ ] DLQ count
- [ ] Duplicate processing
- [ ] Message loss

---

## 11. Benchmark scenarios

### Scenario A — Slow Payment

```text
Payment delay = 1s
```

Compare sync vs async API response.

### Scenario B — Slow Notification

```text
Notification delay = 500ms
```

Verify notification does not increase Order API latency.

### Scenario C — Slow Analytics

```text
Analytics delay = 1s
```

Verify analytics does not affect critical flow.

### Scenario D — Concurrency

Run:

```text
10 concurrent
50 concurrent
100 concurrent
500 concurrent
```

### Request counts

Prefer:

```text
100
1,000
10,000
```

At minimum.

---

# P2 — MQ COMPARISON

## 12. Prove RabbitMQ is the appropriate choice

The assignment explicitly asks for a choice among:

- RabbitMQ
- Apache ActiveMQ
- Apache Kafka

and asks for proof based on knowledge, experience and experiment.

### README section

```text
## Why RabbitMQ?

### Requirements

### RabbitMQ

### Apache Kafka

### Apache ActiveMQ

### Comparison

### Experimental Results

### Final Decision
```

### Compare

| Requirement | RabbitMQ | Kafka | ActiveMQ |
|---|---|---|---|
| Work queue | Excellent | Good | Excellent |
| Per-message routing | Excellent | Good | Good |
| Retry/DLX | Excellent | More application work | Good |
| Fanout | Excellent | Good | Good |
| Event replay | Limited | Excellent | Limited |
| Analytics/event streaming | Good | Excellent | Good |
| Operational complexity | Medium | Higher | Medium |
| Fit for this assignment | Strong | Possible but more complex | Possible |

### Key argument to establish

RabbitMQ is appropriate because this problem is primarily about:

- asynchronous work distribution
- service decoupling
- routing
- retries
- DLQ
- independent consumers
- short critical-path responses

Kafka is particularly strong for high-throughput event streaming and replay, which is useful for Analytics, but introducing Kafka solely for the current transactional work-queue problem may add unnecessary complexity.

Do not claim RabbitMQ is universally better than Kafka.

---

# P2 — DOCKER / KUBERNETES

## 13. Docker

### Tasks

- [ ] All services have Dockerfiles
- [ ] Docker Compose starts complete system
- [ ] RabbitMQ configured through environment variables
- [ ] PostgreSQL configured through environment variables
- [ ] No hard-coded secrets
- [ ] Health checks where appropriate
- [ ] Service dependencies documented

---

## 14. Kubernetes

The exercise context states Docker + Kubernetes.

Minimum manifests:

```text
k8s/
├── namespace.yaml
├── rabbitmq.yaml
├── postgres.yaml
├── api-gateway.yaml
├── order-service.yaml
├── payment-service.yaml
├── inventory-service.yaml
├── notification-service.yaml
└── analytics-service.yaml
```

### Minimum Kubernetes configuration

- [ ] Deployment
- [ ] Service
- [ ] Replicas
- [ ] Resource requests
- [ ] Resource limits
- [ ] Readiness probe
- [ ] Liveness probe
- [ ] Environment variables
- [ ] Secrets
- [ ] Graceful shutdown

Do not over-engineer Kubernetes for the assignment.

---

# P2 — SECURITY / CONFIGURATION

## 15. Remove hard-coded credentials

Replace:

```text
guest / guest
hard-coded tokens
hard-coded secrets
```

with:

```text
.env
.env.example
```

Environment variables:

```text
RABBITMQ_URL
DATABASE_URL
JWT_SECRET
```

For Kubernetes:

```text
Secret
ConfigMap
```

### Tasks

- [ ] Remove hard-coded RabbitMQ credentials
- [ ] Remove hard-coded authentication tokens
- [ ] Add `.env.example`
- [ ] Verify `.env` is gitignored
- [ ] Verify no secrets exist in Git history if applicable

---

# P2 — CODE QUALITY

## 16. Service structure

Aim for:

```text
service/
├── src/
│   ├── controllers/
│   ├── services/
│   ├── repositories/
│   ├── consumers/
│   ├── publishers/
│   ├── infrastructure/
│   │   └── rabbitmq/
│   ├── models/
│   ├── middleware/
│   ├── config/
│   └── app.js
└── tests/
```

Avoid:

- [ ] Business logic in controllers
- [ ] DB logic in controllers
- [ ] RabbitMQ connection logic scattered everywhere
- [ ] Global mutable state for production data
- [ ] Module-import side effects
- [ ] Hard-coded configuration

---

# P3 — README / DOCUMENTATION

## 17. Rewrite README around the assignment

Recommended structure:

```text
# RabbitMQ Evaluation for E-commerce Microservices

## 1. Problem Statement

## 2. Current Architecture

## 3. Problems

## 4. Requirements

## 5. Candidate Technologies

## 6. RabbitMQ vs Kafka vs ActiveMQ

## 7. Why RabbitMQ?

## 8. Proposed Architecture

## 9. RabbitMQ Topology

## 10. Order Flow

## 11. Inventory Concurrency

## 12. Payment Retry

## 13. Saga

## 14. Idempotency

## 15. Transactional Outbox

## 16. Failure Scenarios

## 17. Testing

## 18. Benchmark Methodology

## 19. Benchmark Results

## 20. Limitations

## 21. Conclusion
```

---

# 18. Architecture diagram

README should contain at least one complete architecture diagram:

```text
                         Client
                           |
                           v
                    +--------------+
                    | API Gateway  |
                    +------+-------+
                           |
                           v
                    +--------------+
                    | Order Service|
                    +------+-------+
                           |
                    PostgreSQL
                           |
                      Outbox
                           |
                           v
                     +---------+
                     | RabbitMQ|
                     +----+----+
                          |
          +---------------+---------------+
          |               |               |
          v               v               v
     Inventory         Payment         Analytics
          |               |
          |               +--> Retry
          |               +--> DLX
          |               +--> Parking
          |
          v
      Reservation

                     Notification
                           ^
                           |
                     Async Event
```

---

# 19. Final regression checklist

## Functional

- [ ] Create order works
- [ ] Order returns 202 quickly
- [ ] Order is persisted
- [ ] Inventory reservation works
- [ ] Inventory cannot oversell
- [ ] Payment works
- [ ] Payment retries
- [ ] Permanent Payment failure reaches parking queue
- [ ] Notification is asynchronous
- [ ] Analytics is asynchronous
- [ ] Saga reaches correct final state

## Reliability

- [ ] Duplicate messages are safe
- [ ] RabbitMQ reconnect works
- [ ] No silent message loss
- [ ] No invalid Order state transition
- [ ] No payment charge when inventory fails
- [ ] No overselling across multiple processes

## Testing

- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Happy path passes
- [ ] Inventory failure passes
- [ ] Payment retry passes
- [ ] Payment permanent failure passes
- [ ] Duplicate event passes
- [ ] Concurrent inventory test passes
- [ ] Outbox failure scenario passes if implemented

## Performance

- [ ] Sync vs async API latency measured
- [ ] p50 measured
- [ ] p95 measured
- [ ] p99 measured
- [ ] Throughput measured
- [ ] Error rate measured
- [ ] E2E completion time measured
- [ ] Slow Payment tested
- [ ] Slow Notification tested
- [ ] Slow Analytics tested

## Deployment

- [ ] Docker Compose works
- [ ] Environment variables used
- [ ] No hard-coded credentials
- [ ] Kubernetes manifests work or are validated
- [ ] Readiness probe exists
- [ ] Liveness probe exists
- [ ] Graceful shutdown works

## Documentation

- [ ] RabbitMQ choice explained
- [ ] Kafka comparison included
- [ ] ActiveMQ comparison included
- [ ] Trade-offs documented
- [ ] Benchmark methodology documented
- [ ] Benchmark limitations documented
- [ ] Architecture diagram included
- [ ] Failure scenarios documented

---

# Definition of Done

The project is ready to submit when all of the following are true:

1. Order Service persists orders to PostgreSQL.
2. Order API returns quickly without waiting for downstream services.
3. Inventory reservation is safe under concurrent requests and multiple service instances.
4. Payment has retry + DLX + parking queue.
5. Duplicate messages do not cause duplicate business side effects.
6. Saga does not allow `Payment = SUCCESS` + `Inventory = FAILED` without compensation.
7. RabbitMQ failure does not silently switch production traffic to an in-memory broker.
8. Integration tests verify real RabbitMQ behavior.
9. Benchmark methodology compares equivalent metrics.
10. README clearly explains why RabbitMQ is appropriate compared with Kafka and ActiveMQ.
11. Docker setup works.
12. Kubernetes deployment assumptions are addressed.
13. No hard-coded secrets remain.
14. All tests pass after a clean environment rebuild.

---

# Recommended implementation order

Do NOT implement everything in arbitrary order.

Use this order:

```text
1. RabbitMQ lifecycle
       ↓
2. PostgreSQL
       ↓
3. Order Repository
       ↓
4. Inventory DB + atomic reservation
       ↓
5. Saga redesign
       ↓
6. Idempotency
       ↓
7. Payment retry/DLQ integration tests
       ↓
8. Transactional Outbox
       ↓
9. Full integration tests
       ↓
10. Benchmark
       ↓
11. RabbitMQ/Kafka/ActiveMQ comparison
       ↓
12. Kubernetes
       ↓
13. README
       ↓
14. Final regression
```

---

# Priority guide if time is limited

## MUST HAVE

- [ ] PostgreSQL persistence
- [ ] Correct Saga
- [ ] Atomic Inventory reservation
- [ ] Idempotency
- [ ] Real Payment retry/DLQ test
- [ ] RabbitMQ lifecycle fix
- [ ] RabbitMQ vs Kafka vs ActiveMQ comparison
- [ ] Correct benchmark methodology

## SHOULD HAVE

- [ ] Transactional Outbox
- [ ] Kubernetes manifests
- [ ] Reconnect handling
- [ ] More integration tests

## NICE TO HAVE

- [ ] Advanced observability
- [ ] Redis distributed locking
- [ ] Full Kubernetes production setup
- [ ] Large-scale load testing

---

# Final target

Current:

```text
~67%
```

Target after the fixes:

```text
~85–90%
```

The goal is NOT to maximize the number of technologies.

The goal is to demonstrate:

```text
Correctness
    +
Reliability
    +
Decoupling
    +
Failure handling
    +
Concurrency safety
    +
Evidence-based MQ selection
```

That is what should make the submission convincing to a senior backend reviewer.
