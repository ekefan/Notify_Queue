from prometheus_client import Counter, Gauge, Histogram


JOBS_BY_STATUS = Gauge(
    "notify_jobs_by_status", "Authoritative jobs grouped by status", ["status"]
)
PUBLISHED = Counter("notify_messages_published_total", "Messages published to RabbitMQ")
PUBLISH_FAILURES = Counter(
    "notify_message_publish_failures_total", "RabbitMQ publish failures"
)
DELIVERIES = Counter(
    "notify_delivery_results_total", "Notification delivery outcomes", ["status", "channel"]
)
PROCESSING_SECONDS = Histogram(
    "notify_processing_seconds",
    "Time spent processing a notification",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10),
)
QUEUE_WAIT_SECONDS = Histogram(
    "notify_queue_wait_seconds", "Time from scheduled availability to consumer start"
)
INFLIGHT = Gauge("notify_worker_inflight", "Messages currently handled by this worker")
REDELIVERIES = Counter("notify_message_redeliveries_total", "RabbitMQ redeliveries")
RETRIES = Counter("notify_retries_total", "Jobs scheduled for retry")
DEAD_LETTERS = Counter("notify_dead_letters_total", "Jobs moved to dead-letter status")
RATE_LIMIT_DEFERRALS = Counter(
    "notify_rate_limit_deferrals_total", "Jobs deferred by recipient rate limiting"
)
DB_POOL_SIZE = Gauge("notify_db_pool_size", "Configured SQLAlchemy pool size")
DB_POOL_CHECKED_OUT = Gauge(
    "notify_db_pool_checked_out", "SQLAlchemy connections currently checked out"
)
DB_POOL_OVERFLOW = Gauge("notify_db_pool_overflow", "SQLAlchemy overflow connections")


def observe_pool(pool) -> None:
    DB_POOL_SIZE.set(pool.size())
    DB_POOL_CHECKED_OUT.set(pool.checkedout())
    DB_POOL_OVERFLOW.set(max(pool.overflow(), 0))
