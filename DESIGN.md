A realistic example:
- 5,000 users × 10 notifications/day = 50,000/day
- Average throughput: approximately 0.6 notifications/second
- At a 10× peak: approximately 6/second
A small PostgreSQL deployment can handle that with:
- An index-friendly due-job query
- Small batch claims
- 5–10 worker tasks
- Worker leases for crash recovery
- Reused HTTP clients
- Archived terminal jobs
- Proper connection-pool limits
- Concurrency and load testing
Consider RabbitMQ when measurements show:
- Sustained throughput reaches hundreds of notifications/second
- Queue age continuously increases during expected peak load
- Claim-query p95 remains above 10 ms after basic tuning
- PostgreSQL CPU remains above roughly 70%
- Database contention affects API latency
- Large bursts must survive worker/provider outages
- Worker routing and backpressure become operational requirements

# Version 2: PostgreSQL and RabbitMQ

## Objective

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

## Scheduling future jobs

Immediate jobs are published as soon as their outbox event is available. For this demo,
future jobs and retries use RabbitMQ's delayed-message exchange and carry the delay
between `send_at` and publication time. The delayed-message plugin must be enabled in
the RabbitMQ image and documented as an infrastructure dependency.

This is a deliberate demo trade-off. Keeping millions of long-lived delayed messages
in RabbitMQ increases broker recovery and storage pressure. A production system with
large, far-future schedules would normally keep those schedules in PostgreSQL and use
a small due-job publisher. The load comparison must state this boundary rather than
claim that the broker removes time-based scheduling work in every deployment.

## Priority

Declare the durable work queue with `x-max-priority`, using a small fixed range such as
0 for low, 1 for normal, and 2 for high. The producer maps the persisted job priority
onto the AMQP message priority. Among messages that are ready in RabbitMQ, higher
priority messages are delivered first.

Priority cannot preempt a message already delivered to a consumer. Keep consumer
prefetch bounded so a worker does not reserve a large low-priority buffer while urgent
work waits in RabbitMQ. Start with prefetch equal to, or slightly above, the worker's
actual delivery concurrency and validate priority behavior under load.

## Recipient rate limiting

RabbitMQ does not provide arbitrary per-recipient hourly limits. Immediately before a
provider call, the consumer atomically reserves recipient capacity in PostgreSQL (or
Redis in a later measured optimization). If capacity is unavailable, it does not count
as a delivery attempt. The consumer publishes a delayed replacement for the earliest
eligible time, waits for publisher confirmation, persists the deferred state, and then
acknowledges the current message.

The reservation must include in-flight sends, not only completed sends; otherwise many
consumers can simultaneously observe the final available slot. Failed sends release
their reservation, successful sends convert it to a recorded delivery, and abandoned
reservations expire.

## Consumer workflow

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

## Job states

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

## Demo-visible processing state

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

## Observability

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

## Verification and comparison

Before presenting Version 2, test:

- concurrent duplicate API submissions create one job and one logical publication;
- RabbitMQ redelivery does not produce a duplicate delivery record;
- high-priority ready work overtakes low-priority ready work with bounded prefetch;
- concurrent consumers cannot exceed the last recipient capacity slot;
- delayed jobs are not delivered before `send_at`;
- retry delays grow exponentially and exhausted jobs reach the dead-letter exchange;
- killing a consumer before acknowledgement causes safe redelivery;
- losing RabbitMQ after the database commit is repaired by the publish outbox;
- webhook failure never causes notification redelivery.

Run the same workload against Version 1 and Version 2. Capture throughput, claim or
delivery latency, oldest-due age, database pressure, RabbitMQ depth, and recovery after
failure. Version 1 explains the database ranking bottleneck; Version 2 demonstrates
broker priority, buffering, acknowledgement, and backpressure behavior.
