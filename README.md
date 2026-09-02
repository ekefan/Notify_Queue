# Notify Queue


[![Notify Queue demo](https://vumbnail.com/1223282070.jpg)](https://vimeo.com/1223282070)


Notify Queue is a distributed delayed job and notification delivery system.
This implementation supports future delivery, priorities, concurrent workers, idempotent
submission, configurable mock failures, exponential retries, dead-letter status,
recipient limits, callbacks, status inspection, and operational metrics.

The repository contains two implementations:

- **v1** Standard submission, which uses PostgreSQL `FOR UPDATE SKIP LOCKED` polling. It demonstrates safe concurrent claims and the scaling cost of repeatedly ranking a large due set.

- **v2** keeps PostgreSQL as the source of truth and uses a durable publish outbox plus
  RabbitMQ for ready-work priority, buffering, acknowledgements, and redelivery. V2 was
  implemented to test the possibility of improving performance by decoupling and scaling.

See [DESIGN.md](DESIGN.md) for architecture decisions, trade-offs, failure windows,
and scaling analysis.

Future jobs remain in the indexed outbox until `available_at`. RabbitMQ therefore
contains only work that is ready to process. Consumers acknowledge a message only
after its durable state transition.

## Requirements

- Docker with the Compose plugin installed.
- `uv` and Python 3.14 when running services or tests on the host.
- Ports `5432`, `5672`, `15672`, `15692`, `8000`, `9090`, and `3000` available.

## First-time setup

```bash
bash install_requirements.sh
cp .env.example .env
cp .envdev.example .env.dev
```

The installer adds `make` when missing and installs all locked dependencies. Later,
run `make requirements` to refresh them.

### Run on the host or run v1

```bash
make postgres-up
make migrate
make producer-api
```

## Seed DB
```bash
uv run python seed.py --count 100
```

For the PostgreSQL consumer run:

```bash
make consumer-v1
```

## Schedule a job

The request requires a unique `Idempotency-Key`:

```bash
curl -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-email-001' \
  -d '{
    "recipient": "person@example.com",
    "channel": "email",
    "payload": {
      "subject": "Notify Queue demo",
      "body": "Hello from RabbitMQ"
    },
    "send_at": "2026-09-01T23:00:00Z",
    "priority": 2
  }'
```

Priority values are:

| Value | Priority |
|---:|---|
| `0` | low |
| `1` | normal |
| `2` | high |

Submitting the same idempotency key again returns `409 Conflict` with the original job
reference instead of creating a duplicate.

## Inspect jobs

```bash
curl http://localhost:8000/jobs/JOB_ID/status
curl http://localhost:8000/jobs/metrics
curl http://localhost:8000/metrics
```

The lightweight product metrics endpoint reports pending, processing, sent, failed,
and dead-lettered counts. <br>
`/metrics` exposes Prometheus text format.

## Tests

Run the complete suite:

```bash
make test
```

Integration tests use a disposable PostgreSQL Testcontainer and therefore require
Docker. The suite covers:

- concurrent idempotent submissions;
- twelve workers competing for one job;
- non-overlapping concurrent batch claims;
- priority ordering and future-job exclusion;
- within-batch recipient deferral;
- expired-claim recovery;
- exponential retry timing and dead-letter transition;
- one durable outbox event for concurrent duplicate submissions;
- RabbitMQ job-message serialization.

## Exactly-once boundary

`FOR UPDATE SKIP LOCKED` in v1 and conditional job claiming in v2 prevent two workers
from concurrently owning the same database job. The idempotency-key constraint prevents
duplicate scheduling.

No queue or database lock can by itself guarantee exactly-once effects at an external
provider. A worker can send successfully and crash before recording success. A real
provider must accept the stable job ID as an idempotency key. The resulting contract is
at-least-once dispatch with exactly-once external effect when the provider honors that
key.


### Run v2 (recommended architecture)

```bash
make v2-up
## if it fails because of used port run:
make v2-up-port-8001
```

This starts the full stack. The API applies all committed migrations with `alembic
upgrade head` before it starts.

| Service | Address |
|---|---|
| API / OpenAPI | `http://localhost:8000` / `/docs` |
| RabbitMQ management | `http://localhost:15672` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |

RabbitMQ uses `notify_queue` / `notify_queue`; Grafana uses `admin` / `admin`.
Use `make v2-logs` to follow logs and `make v2-down` to stop the stack.


You can create multiple jobs through the API so the v2 publisher can enqueue them:

```bash
./add_job.sh 100
```
