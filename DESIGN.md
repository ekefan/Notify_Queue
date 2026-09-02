
# Architecture evolution

## Initial architecture: PostgreSQL polling

```text
Client
  │
  ▼
FastAPI ──transaction──> PostgreSQL jobs
                              ▲
                              │ poll, rank, lock, and claim
                              │
                    ┌─────────┴─────────┐
                    │                   │
                 worker 1           worker N
                    │                   │
                    └───────┬───────────┘
                            ▼
                   notification provider
                            │
                            ▼
                  PostgreSQL status update
```

Version 1 uses PostgreSQL as both the authoritative state store and the work queue.
Workers repeatedly search the `jobs` table for due `pending` or `failed` rows, rank
them by priority and age, claim a small batch with `FOR UPDATE SKIP LOCKED`, and process
the batch concurrently. This design is simple, durable, and avoids introducing a
separate broker. Multiple workers can claim safely without processing the same row at
the same time, and a recovery loop returns abandoned `processing` jobs to `pending`.

### Scaling bottleneck

The limiting operation is not inserting a job; it is repeatedly discovering the next
jobs to run. Every polling worker asks PostgreSQL to filter the growing jobs table,
identify due rows, calculate their effective priority, order the candidates, acquire
row locks, and update the winners. When no work is available, workers still issue the
polling query. Adding workers increases both delivery capacity and database polling,
sorting, locking, and connection pressure.

At low and moderate traffic this is a reasonable trade-off. As the due set and worker
count grow, however, more application capacity produces more contention on the same
database that also serves API requests and stores job state. Large bursts can make
workers repeatedly rank a large eligible set, increase claim latency, consume the
connection pool, and affect API latency. PostgreSQL also has to provide queue concerns
such as priority selection, crash recovery, and backpressure in addition to its role as
the system of record.

A realistic starting workload is:

- 5,000 users × 10 notifications/day = 50,000/day
- Average throughput: approximately 0.6 notifications/second
- At a 10× peak: approximately 6/second

A PostgreSQL deployment can handle that with:

- An index-friendly due-job query
- Small batch claims
- 5–10 worker tasks
- Worker leases for crash recovery
- Reused HTTP clients
- Archived terminal jobs
- Proper connection-pool limits
- Concurrency and load testing


## V2 architecture: PostgreSQL and RabbitMQ

### Objective

Version 1 demonstrates the cost of repeatedly asking PostgreSQL to rank a large due
set. Version 2 keeps PostgreSQL as the authoritative job-state store and moves ready
work distribution, priority ordering, acknowledgements, and consumer buffering to
RabbitMQ.

```text
Client
  │
  ▼
API ──transaction──> jobs + publish_outbox
                           │
                           ▼
                    outbox publisher
                           │ publisher confirm
                           ▼
                    RabbitMQ exchange
                           │
                    durable priority queue
                           │ prefetch
                           ▼
                       consumers
                           │
                           ├──> notification provider
                           └──> PostgreSQL status + webhook outbox
```

The logical API operation is “save job, then push to the queue.” A direct database
commit followed by a RabbitMQ publish is not atomic: a process crash between those
operations leaves a saved job that is never queued. Therefore, the API writes the job
and a `publish_outbox` row in one PostgreSQL transaction. A lightweight publisher in
the producer service sends unpublished events and records RabbitMQ publisher confirms.
This relay is not a scheduling policy engine; it only closes the database-to-broker
failure window.

### Scheduling future jobs

The API inserts a `publish_outbox` event with `available_at = send_at` in the same
transaction as the job. A single lightweight publisher selects a bounded batch from
the partial `(available_at, created_at) WHERE published_at IS NULL` index. It publishes
only events that are due and records the RabbitMQ publisher confirmation.

This means RabbitMQ contains only ready work; it does not retain millions of long-lived
delayed messages. The publisher still performs time-based scheduling, but it scans a
narrow append-oriented outbox with one indexed query rather than having every worker
rank and update the large jobs table. Multiple publisher instances can safely use
`FOR UPDATE SKIP LOCKED` if publisher throughput later requires horizontal scaling.

There is an unavoidable duplicate-publication window if the process dies after the
broker confirms a message but before `published_at` commits. Consumers therefore treat
messages as at least once and conditionally claim the authoritative PostgreSQL job.

### Priority

I declared the durable work queue with `x-max-priority`, using a small fixed range such as
0 for low, 1 for normal, and 2 for high. The producer maps the persisted job priority
onto the AMQP message priority. Among messages that are ready in RabbitMQ, higher
priority messages are delivered first.

Priority cannot preempt a message already delivered to a consumer. Keep consumer
prefetch bounded so a worker does not reserve a large low-priority buffer while urgent
work waits in RabbitMQ. Start with prefetch equal to, or slightly above, the worker's
actual delivery concurrency and validate priority behavior under load.

### Recipient rate limiting

RabbitMQ does not provide arbitrary per-recipient hourly limits. Immediately before a
provider call, the consumer atomically reserves recipient capacity in PostgreSQL. 
If capacity is unavailable, it does not count
as a delivery attempt. The consumer publishes a delayed replacement for the earliest
eligible time, waits for publisher confirmation, persists the deferred state, and then
acknowledges the current message.

The reservation must include in-flight sends, not only completed sends; otherwise many
consumers can simultaneously observe the final available slot. Failed sends release
their reservation, successful sends convert it to a recorded delivery, and abandoned
reservations expire.

### Consumer workflow

Consumers receive a small prefetched buffer from RabbitMQ. For each message they:

1. Load the job by ID and verify that its persisted state is deliverable.
2. Atomically transition it to `processing` and acquire recipient capacity.
3. Run the mock or real sender outside the database transaction.
4. On success, commit `sent`, the delivery record, rate-limit usage, and a webhook
   outbox event; then acknowledge the RabbitMQ message.
5. On retryable failure, persist `failed` with exponential backoff, publish a delayed
   retry with confirmation, and acknowledge the current message.
6. At the retry cap, persist `dead_lettered`, publish the status webhook event, and
   reject the message to the dead-letter exchange.

Messages are never acknowledged before their durable state transition. A consumer
crash leaves the message unacknowledged, so RabbitMQ redelivers it. Redelivery is
expected and must be harmless: the database state, a unique delivery record, and the
job ID used as the provider idempotency key protect the externally visible effect.

### Job states

```text
scheduled ──published/delay elapsed──> queued ──delivered──> processing
                                                        │
                              success───────────────────┼──> sent
                              retryable failure─────────┼──> failed ──> queued
                              attempts exhausted────────└──> dead_lettered
```

RabbitMQ message state and PostgreSQL job state can temporarily differ during crashes.
Consumers always consult PostgreSQL before sending, and outbox publication is
idempotent, so duplicate messages do not imply duplicate notification effects.

### Demo-visible processing state

Normal mock latency remains configurable in milliseconds. To make the `processing`
metric observable during a presentation, 20 percent of mock deliveries take a random
duration up to three seconds by default:

```env
MOCK_LONG_PROCESSING_RATE=0.20
MOCK_LONG_PROCESSING_MAX_SECONDS=3.0
```

This delay is a presentation/load-test setting, not a correctness mechanism. Metrics
must be read while jobs are actively being consumed; no production state transition
should be artificially delayed merely to make a counter non-zero.

### Observability

Version 2 includes Prometheus and Grafana. The minimum dashboard shows:

- API request rate and p50/p95/p99 latency;
- RabbitMQ ready, unacknowledged, publish, acknowledgement, and redelivery rates;
- queue depth and oldest-message age by priority;
- jobs by persisted status;
- delivery throughput and latency;
- retry, dead-letter, and recipient-rate-limit deferral rates;
- consumer concurrency, utilization, and prefetch saturation;
- PostgreSQL pool usage, query latency, locks, CPU, and connections;
- webhook success, failure, and retry rates.

Services emit structured logs with stable events such as `job.scheduled`,
`outbox.published`, `message.received`, `job.processing`, `delivery.succeeded`,
`delivery.failed`, `rate_limit.deferred`, `job.dead_lettered`, and
`message.acknowledged`. Include job ID, worker ID, attempt, priority, trace ID, and
duration where applicable. Hash recipients and never log notification payloads.

### Measured v2 publisher baseline

On the local Docker environment, the complete due-outbox publication path produced:

- 683.3 messages/second with sequential publisher confirmations;
- 1,422.4 messages/second with confirmation concurrency 25;
- 1,506.3 messages/second with confirmation concurrency 50 and batch size 1,000.

Each run published 10,000 persistent messages into a dedicated RabbitMQ priority queue
and committed `published_at` in an isolated PostgreSQL database. The small gain from 25
to 50 indicates the local system was near a database/broker persistence knee rather
than limited only by application concurrency. This is a publisher-stage benchmark,
not an end-to-end delivery capacity claim.
