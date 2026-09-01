# Notify Queue — Design and Implementation Plan

## 1. Purpose

Notify Queue schedules email, SMS, and push notifications for immediate or future
delivery. The first implementation uses PostgreSQL as both the durable source of truth
and the work queue. A second implementation adds RabbitMQ as the dispatch mechanism
while preserving PostgreSQL as the business-state store.

This document is an implementation plan, not implementation code.

## 2. Scope

The system must provide:

- job scheduling by absolute send time or relative delay;
- priority ordering among due jobs;
- safe work claiming across concurrent worker instances;
- idempotent job submission;
- configurable mock-delivery failure;
- retries with exponential backoff and a retry cap;
- dead-letter handling for exhausted jobs;
- a per-recipient rolling-hour rate limit;
- status-change webhook callbacks;
- job-status and aggregate-metrics endpoints;
- a repeatable constrained-environment load experiment.

Authentication, multi-tenancy, real notification providers, templates, and a webhook
administration API are deliberately outside the assessment scope.

## 3. Proposed repository boundaries

Keep transport concerns separate from business rules so the PostgreSQL and RabbitMQ
versions can reuse most of the system.

```text
src/
  common/       configuration, database session, time utilities, domain states
  producer/     HTTP API, request validation, scheduling and idempotency service
  worker/       delivery workflow, rate limiter, retry policy, mock sender
  callbacks/    webhook outbox dispatcher and mocked callback receiver
  metrics/      Prometheus instrumentation
load/           job generator and experiment scenarios
tests/          unit, integration, concurrency, and load-smoke tests
observability/  Prometheus and Grafana configuration/dashboards
```

Implementation two should add a RabbitMQ publisher/consumer adapter rather than create
a second copy of the domain model and delivery workflow.

## 4. System components

### API/producer

Validates requests, resolves `scheduled_for` from either an absolute time or delay,
creates jobs, enforces submission idempotency, and exposes status and metrics.
Successful scheduling means the job is durable, not that it has been delivered, so the
API returns `202 Accepted`.

### PostgreSQL

Holds authoritative job state, idempotency keys, worker leases, delivery records,
recipient rate-limit state, dead-letter metadata, and webhook outbox events.

### Worker

Claims a small batch of due jobs, commits the claims quickly, and delivers outside the
claim transaction. Multiple identical worker processes may run concurrently.

### Mock sender

Waits for a configurable latency and fails at a configurable probability. It records a
delivery in a unique delivery ledger so tests can observe whether duplicate delivery
occurred.

### Webhook dispatcher

Reads callback events from an outbox and calls the configured callback URL. Callback
retries are independent from notification retries; a callback failure must never cause
another notification delivery.

### Observability stack

Prometheus scrapes application, worker, and PostgreSQL metrics. Grafana displays
throughput, queue lag, failures, saturation, and resource use during experiments.

## 5. API contract

### `POST /schedule`

Request fields:

- `recipient`: string; do not require UUID because email addresses and phone numbers
  are valid recipients;
- `channel`: `email`, `sms`, or `push`;
- `payload`: channel-specific JSON object;
- exactly one of `scheduled_for` or `delay_seconds`;
- `priority_level`: `low`, `normal`, or `high`;
- `Idempotency-Key` header.

Response fields include job ID, status, resolved schedule time, and whether the response
was deduplicated. A repeated key returns the original job rather than creating a second
one.

### `POST /webhook/jobs`

Acts as the mocked destination for callback events. It accepts job ID, status, event ID,
attempt number, occurrence time, and optional error information. The event ID is itself
idempotent because webhook delivery is also at least once.

### `GET /jobs/{id}/status`

Returns status, priority, schedule time, attempt count, next eligible time, delivery
time, and last error. It must not return internal lock tokens or sensitive payloads.

### `GET /jobs/metrics`

Returns lightweight status counts required by the assessment. Prometheus metrics remain
on a separate `/metrics` endpoint because operational time-series data and the product
API serve different purposes.

## 6. PostgreSQL data model

### `jobs`

Important columns:

- UUID primary key;
- idempotency key with a unique constraint;
- recipient, channel, and JSON payload;
- priority;
- `scheduled_for` and `available_at` in UTC;
- status;
- attempt count and maximum attempts;
- claim token, worker ID, and lease expiry;
- last error;
- created, updated, and delivered timestamps.

`available_at` is the next time a worker may claim the job. Initially it equals the
resolved schedule time. Retry and rate-limit deferral update this field.

The critical claim index begins with status and availability, followed by priority and
creation time. Finished jobs should eventually be archived or partitioned so they do
not enlarge the active scheduling index indefinitely.

### `deliveries`

Contains one row per successful mock delivery. A unique constraint on job ID prevents
duplicate observable sends in concurrency tests. Store recipient and delivery time to
support auditing and rate-limit verification.

### `recipient_rate_limits`

Maintains rate-limit buckets per recipient. Workers lock the relevant recipient row
before reserving capacity. This prevents two workers from both observing the last
available slot and exceeding the limit.

For exact rolling-window behavior, successful-delivery timestamps are authoritative.
For higher throughput, use fixed or token-bucket windows with a documented boundary
trade-off. The assessment version can favor correctness and clarity over maximum rate.

### `dead_letters`

Stores job ID, final error, attempts, and dead-letter time. The job also retains
`dead_lettered` status. Keeping explicit dead-letter metadata makes inspection and a
future replay operation straightforward.

### `webhook_outbox`

Contains immutable status-change events, destination, attempt count, next attempt time,
and delivered time. Event IDs are unique.

## 7. Job state machine

```text
scheduled ──due──> processing ──success──> sent
                       │
                       ├──retryable failure──> failed ──backoff elapsed──> processing
                       │
                       └──permanent/exhausted failure──> dead_lettered

processing ──lease expired──> eligible for reclaim
scheduled/failed ──rate limited──> same state with later available_at
```

State transitions and their webhook-outbox event must be committed in the same database
transaction.

## 8. Safe concurrent claiming

For implementation one, each worker performs the following:

1. Begin a short database transaction.
2. Select a bounded batch of due `scheduled` or retryable `failed` jobs.
3. Order by priority descending, then `available_at` ascending, then creation time.
4. Lock rows with PostgreSQL `FOR UPDATE SKIP LOCKED`.
5. Mark the selected rows `processing` with a unique claim token and lease expiry.
6. Commit immediately.
7. Perform rate-limit checks and provider calls outside the claim transaction.
8. Finalize only when the stored claim token still belongs to that worker.

`SKIP LOCKED` prevents concurrent workers from selecting the same rows. The lease allows
recovery after a worker crash. Long deliveries either need a comfortably sized lease or
a heartbeat that renews it. Batch size should remain small enough to avoid claiming far
more work than one worker can finish before its leases expire.

## 9. Exactly-once: precise guarantee

The system can guarantee exactly-once scheduling and exactly-once database
finalization. The mock sender can demonstrate exactly-once observable delivery by
inserting into `deliveries(job_id UNIQUE)` transactionally.

Strict exactly-once delivery to a real external provider is impossible unless that
provider participates in the transaction or honors an idempotency key. There is an
unavoidable crash window after the provider accepts a request but before the worker
records success. The production contract is therefore:

- at-least-once dispatch;
- stable job ID as the provider idempotency key;
- exactly-once effect when the provider honors that key.

This distinction should be stated explicitly during the presentation. Claim-based
locking alone does not solve the external side-effect problem.

## 10. Idempotent submission

1. Require an `Idempotency-Key` header for scheduling.
2. Insert the job and idempotency key in one transaction.
3. Enforce uniqueness in PostgreSQL rather than using an application-only pre-check.
4. On conflict, return the existing job.
5. Optionally store a request fingerprint. Reject reuse of the same key with a different
   payload to avoid silently returning an unrelated job.

The database constraint closes the race between simultaneous identical requests.

## 11. Priority and starvation

Among jobs that are already due, workers claim high priority before normal and low
priority. Future high-priority jobs must not block currently due lower-priority jobs.

Strict priority can starve low-priority work under continuous high-priority traffic.
Use strict priority for the minimum version and expose queue age by priority. A later
version can use priority ageing or weighted batch allocation if starvation appears.

## 12. Recipient rate limiting

The required behavior is deferral, not rejection.

1. A claimed job requests capacity for its recipient.
2. The worker atomically locks/reserves that recipient's capacity.
3. If capacity exists, delivery proceeds.
4. If the limit has been reached, calculate the earliest next eligible time.
5. Return the job to a queued state with that `available_at` value.
6. Do not increment the delivery attempt count.

Holding a recipient lock across a slow provider call reduces concurrency. A reservation
with a short expiry avoids that, but unused reservations must be released on failure.
Start with a simple transactionally reserved bucket and test the boundary with many jobs
for one recipient.

## 13. Retries and dead letters

For retryable failures, increment the attempt count and calculate:

```text
next_delay = min(max_delay, base_delay × 2^(attempt - 1)) + random_jitter
```

Store the next eligible time in `available_at`. Jitter prevents synchronized retry
storms. Invalid payloads and other permanent errors can bypass retries. Once the maximum
attempt count is reached, transition atomically to `dead_lettered`, create the dead
letter record, and enqueue the webhook event.

The mock failure probability, mock latency, backoff base, cap, jitter, and maximum
attempts should all be environment configuration so tests are deterministic when
needed.

## 14. Webhook reliability

Do not call the webhook inside the notification finalization transaction. Instead:

1. Change job status and insert an outbox event in one transaction.
2. A callback dispatcher claims due outbox events.
3. It calls `/webhook/jobs` with the event ID.
4. A successful response marks only the callback event delivered.
5. Callback failures retry with their own backoff and cap.

This prevents a slow webhook from holding database locks and prevents callback failure
from triggering duplicate notifications.

## 15. Implementation plan: PostgreSQL version

### Phase 1 — define behavior

1. Finalize request/response schemas and state names.
2. Decide the priority values and recipient rate-limit semantics.
3. Write the database migration and indexes.
4. Document the exact-once boundary and failure assumptions.

Exit condition: the API contract, schema, and state-transition table agree.

### Phase 2 — scheduling API

1. Configure application settings and PostgreSQL connections.
2. Implement absolute-time and delay resolution in UTC.
3. Implement transactional idempotent insertion.
4. Add status lookup and database-backed status counts.
5. Test duplicate concurrent submissions.

Exit condition: jobs persist correctly and duplicate keys produce one row.

### Phase 3 — worker and priority

1. Implement bounded `SKIP LOCKED` claims and leases.
2. Add the configurable mock sender.
3. Record successful mock deliveries uniquely.
4. Finalize jobs only with the matching claim token.
5. Test many workers competing for one job and for a mixed-priority batch.

Exit condition: concurrent workers record exactly one mock delivery per job, and due
high-priority work is claimed first.

### Phase 4 — retries, dead letters, and recovery

1. Classify mock failures as retryable or permanent.
2. Add exponential backoff with jitter.
3. Add maximum attempts and dead-letter transitions.
4. Recover expired claims.
5. Test crash-after-claim, retry timing, and poison-message isolation.

Exit condition: one poison job cannot block later jobs and eventually dead-letters.

### Phase 5 — recipient rate limit

1. Add the recipient capacity/reservation model.
2. Defer excess jobs without consuming attempts.
3. Test concurrent workers at the final available slot.
4. Expose deferral counts and duration as metrics.

Exit condition: concurrency cannot push a recipient above the configured limit.

### Phase 6 — webhook outbox

1. Create status events transactionally.
2. Implement a separate callback dispatcher.
3. Make the mock receiver idempotent by event ID.
4. Test callback failure independently from notification delivery.

Exit condition: callback retries never cause notification redelivery.

### Phase 7 — operational polish

1. Add graceful shutdown so workers stop claiming and finish or release active work.
2. Add structured logs containing job ID, worker ID, attempt, channel, and claim token.
3. Add health/readiness checks for API, workers, database, and dispatchers.
4. Add data retention/cleanup for terminal jobs and old callback events.

## 16. Constrained load-test environment

Use Docker Compose to make the experiment repeatable, not because Docker is an
assessment requirement.

Services:

- API;
- multiple scalable workers;
- PostgreSQL with a small CPU and memory allocation;
- Prometheus;
- Grafana;
- PostgreSQL exporter;
- container metrics collector;
- load controller;
- mock webhook receiver.

Apply explicit CPU and memory limits to both application containers and PostgreSQL.
Configure a deliberately small PostgreSQL connection limit and small application pools.
Pin database storage to a known local volume. Record container runtime, host hardware,
configuration, seed, and test duration so results are reproducible.

Do not begin with an extremely small environment that only measures startup failure.
Establish a stable baseline first, then reduce resources or increase load one variable
at a time.

## 17. Load controller

The controller continuously submits randomized jobs with a reproducible random seed.
It should vary:

- recipient distribution, including a hot-recipient scenario;
- channel;
- priority;
- immediate versus future scheduling;
- idempotency duplicates;
- payload size;
- request rate;
- mock sender latency and failure probability.

Use closed-loop load for latency behavior and open-loop arrival rates for overload
behavior. A closed-loop client slows down when the server slows, which can hide
backpressure failure. The overload experiment therefore needs a target arrival rate
independent of response time.

Each run should have warm-up, steady-state, overload, and recovery stages. Preserve the
same workload seed when comparing PostgreSQL-only and RabbitMQ implementations.

## 18. Metrics and Grafana dashboard

### API

- requests per second by route and status;
- p50, p95, and p99 latency;
- rejected/time-out requests;
- active requests;
- database pool checked-out count and wait time.

### Queue and worker

- scheduled jobs by status and priority;
- due queue depth;
- age of oldest due job, the primary lag signal;
- claim rate, delivery rate, and completion rate;
- active leases and expired-lease recoveries;
- retry and dead-letter rates;
- rate-limit deferrals;
- worker processing latency and utilization.

### PostgreSQL

- active, idle, and waiting connections;
- connection-limit utilization;
- transaction rate;
- lock wait count and duration;
- query p95/p99 latency;
- tuple/index reads and cache-hit ratio;
- disk I/O, WAL rate, checkpoints, CPU, and memory;
- table/index size and dead tuples.

### RabbitMQ, in implementation two

- ready and unacknowledged messages;
- publish, delivery, acknowledgement, and redelivery rates;
- consumer utilization;
- connection/channel count;
- memory/disk alarms;
- dead-letter rate.

The main dashboard should place offered load, accepted load, completed throughput, API
latency, oldest-due age, database pool wait, database connections, and CPU together.
That makes cause and effect visible during saturation.

## 19. Breakpoint experiments

Connection pools are one plausible first bottleneck, not a conclusion. Measure several
failure modes independently.

### Experiment A — API ingestion saturation

Increase schedule requests per second while workers are stopped. Find the rate where
API p99 latency rises sharply or errors begin. This isolates insert/index/connection
capacity from delivery work.

### Experiment B — worker saturation

Preload due jobs, hold API traffic constant, and increase worker replicas. Throughput
should rise, then flatten. Identify whether the plateau correlates with CPU, provider
latency, database locks, pool waits, or I/O.

### Experiment C — connection exhaustion

Use intentionally small pools, then increase replicas. Observe pool wait time before
raising pool sizes. Increasing every pool can overload PostgreSQL because total possible
connections equal pool size multiplied by process count.

### Experiment D — hot-recipient contention

Send most jobs to one recipient. Observe recipient-lock contention and rate-limit
deferrals while unrelated recipients continue processing.

### Experiment E — retry storm

Raise mock failure probability, then restore it. Verify exponential backoff and jitter
prevent a synchronized surge and that the system drains afterward.

### Experiment F — poison workload

Inject permanently failing jobs among valid jobs. Confirm healthy jobs continue and
poison jobs reach the dead-letter state at the cap.

### Experiment G — worker crash and recovery

Terminate workers after claims. Verify leases expire, jobs are reclaimed, and the mock
delivery ledger still has no duplicates.

### Experiment H — database pressure

Constrain PostgreSQL CPU, memory, and I/O separately. Observe whether query latency,
cache misses, checkpoints, or lock waits lead queue lag.

For every experiment, define failure as a measurable service-level boundary—for
example, p99 scheduling latency above one second, errors above one percent, or oldest
due-job age continuously increasing. Report the last stable rate and the first unstable
rate rather than claiming one vague maximum.

## 20. Implementation two: PostgreSQL plus RabbitMQ

RabbitMQ should change dispatch, not correctness ownership.

### Publishing path

1. Scheduling still commits the job to PostgreSQL.
2. A scheduler selects jobs entering the due horizon.
3. It writes a publish event to an outbox transactionally.
4. An outbox publisher sends job IDs to RabbitMQ.
5. Publisher confirms are recorded; unpublished events are retried.

### Consumption path

1. A worker receives a job ID from RabbitMQ.
2. It claims/verifies the corresponding PostgreSQL job.
3. It runs the same rate-limit, sender, retry, finalization, and callback workflow.
4. It acknowledges the broker message only after durable state transition.
5. Redelivery is harmless because database state and unique delivery records remain
   idempotent.

RabbitMQ delayed-message plugins should not be required. Retries and long schedules can
remain governed by PostgreSQL `available_at`, with the scheduler publishing only due or
near-due work. This avoids relying on broker memory for millions of far-future jobs.

### Fair comparison

Run the same seeded workloads, resource limits, sender behavior, and service-level
thresholds against both implementations. Compare:

- API throughput and latency;
- database query and connection pressure;
- due-job lag;
- delivery throughput;
- operational complexity;
- recovery behavior after component failure.

Expected outcome: RabbitMQ may reduce database polling and improve burst absorption,
but it does not remove PostgreSQL coordination, idempotency, or external exactly-once
limitations. At small scale, the simpler PostgreSQL-only version may perform better
operationally because it has fewer moving parts.

## 21. Scaling toward millions of jobs and thousands of workers

Progressive measures:

1. Keep the due-job partial index small and archive terminal jobs.
2. Claim batches rather than one job per transaction.
3. Separate API and worker connection pools; place a pooler such as PgBouncer in front
   of PostgreSQL when process count becomes high.
4. Partition jobs by schedule time once table/index maintenance warrants it.
5. Shard workers by channel/provider to respect independent downstream limits.
6. Publish only near-due jobs to RabbitMQ and keep distant schedules in PostgreSQL.
7. Scale workers from oldest-due age and due depth, not total scheduled count.
8. Introduce Redis only if measured recipient-rate-limit contention justifies moving
   atomic counters out of PostgreSQL.

Thousands of workers cannot each hold a large connection pool. Before that point, total
database connections, lock contention, provider quotas, scheduler throughput, or
RabbitMQ channel counts will impose a limit. The load experiments determine which one
appears first in the constrained deployment.

## 22. Essential verification suite

The submission should include tests proving:

- simultaneous identical idempotency keys create one job;
- many workers contending for one due job produce one mock delivery;
- high priority wins among jobs that are all due;
- future jobs are never claimed early;
- an expired lease is recoverable;
- retries grow exponentially and terminate at the configured cap;
- poison jobs do not block healthy jobs;
- concurrent work respects the last recipient rate-limit slot;
- callback failure cannot cause notification redelivery;
- RabbitMQ redelivery remains harmless in implementation two.

## 23. Recommended execution order

Finish and test the PostgreSQL-only version before adding load infrastructure or
RabbitMQ. The practical order is:

1. contract and schema;
2. scheduling and idempotency;
3. claim and mock delivery;
4. retries and dead letters;
5. rate limits;
6. webhook outbox;
7. correctness/concurrency tests;
8. Prometheus and Grafana;
9. constrained breakpoint experiments;
10. RabbitMQ adapter;
11. repeat the same experiments and compare results.

This order prevents observability and broker complexity from hiding correctness bugs in
the core state machine.
