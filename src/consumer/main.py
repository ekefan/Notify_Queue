import asyncio
import logging
import os
import random
import signal
import uuid

import httpx

from common.database import AsyncSessionLocal
from common.repositories.jobs import JobRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")

WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"

POLL_INTERVAL_SECONDS = float(os.environ.get("WORKER_POLL_INTERVAL_SECONDS", "0.4"))
BATCH_SIZE = int(os.environ.get("WORKER_BATCH_SIZE", "10"))
FAILURE_RATE = float(os.environ.get("MOCK_TRANSIENT_FAILURE_RATE", "0.2"))
BASE_RETRY_DELAY_SECONDS = int(os.environ.get("BASE_RETRY_DELAY_SECONDS", "30"))
MIN_LATENCY_SECONDS = float(int(os.environ.get("MOCK_MIN_LATENCY_MS", "20")) / 1000)
MAX_LATENCY_SECONDS = float(int(os.environ.get("MOCK_MAX_LATENCY_MS", "100")) / 1000)
LONG_PROCESSING_RATE = float(os.environ.get("MOCK_LONG_PROCESSING_RATE", "0.2"))
LONG_PROCESSING_MAX_SECONDS = float(
    os.environ.get("MOCK_LONG_PROCESSING_MAX_SECONDS", "3.0")
)
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://localhost:8000/webhook/receive")
RATE_LIMIT_PER_HOUR = int(os.environ.get("RATE_LIMIT_PER_HOUR", "10"))
STUCK_JOB_TIMEOUT_SECONDS = int(os.environ.get("STUCK_JOB_TIMEOUT_SECONDS", "300"))
RECOVERY_INTERVAL_SECONDS = int(os.environ.get("RECOVERY_INTERVAL_SECONDS", "60"))

_shutdown = asyncio.Event()


async def mock_send(job) -> bool:
    """Stub sender: simulates delivery with a configurable random failure rate."""
    if random.random() < LONG_PROCESSING_RATE:
        latency = random.uniform(MAX_LATENCY_SECONDS, LONG_PROCESSING_MAX_SECONDS)
    else:
        latency = random.uniform(MIN_LATENCY_SECONDS, MAX_LATENCY_SECONDS)
    
    logger.info(
        "mock sending job %s (latency=%.3fs, failure_rate=%.2f)",
        job.id,
        latency,
        FAILURE_RATE,
    )
    await asyncio.sleep(latency)
    return random.random() >= FAILURE_RATE


async def call_webhook(client: httpx.AsyncClient, job_id, status: str) -> None:
    payload = {"job_id": str(job_id), "status": status}
    try:
        await client.post(WEBHOOK_URL, json=payload)
    except httpx.HTTPError as exc:
        logger.warning("webhook call failed for job %s: %s", job_id, exc)


async def process_single_claimed_job(
    job, worker_id: str, http_client: httpx.AsyncClient
) -> None:
    """Processes an individual job instance and records its outcome."""
    logger.info(
        "processing job %s (priority=%s, attempt=%s) on slot %s",
        job.id,
        job.priority,
        job.attempts + 1,
        worker_id,
    )

    success = await mock_send(job)

    async with AsyncSessionLocal() as session:
        repo = JobRepository(session)
        if success:
            await repo.mark_sent(job.id)
            logger.info("Job %s sent successfully", job.id)
            await call_webhook(http_client, job.id, "sent")
        else:
            error_message = "Simulated send failure"
            updated = await repo.mark_failed_or_dead_letter(
                job.id,
                error=error_message,
                base_delay_seconds=BASE_RETRY_DELAY_SECONDS,
            )
            final_status = updated.status if updated else "failed"
            logger.warning("Job %s failed to send: %s", job.id, error_message)
            await call_webhook(http_client, job.id, final_status)


async def process_batch_jobs(
    worker_id: str, http_client: httpx.AsyncClient
) -> int:
    """Claims a batch of jobs and processes them concurrently. Returns count of claimed jobs."""
    async with AsyncSessionLocal() as session:
        repo = JobRepository(session)
        jobs = await repo.claim_next_jobs(
            worker_id=worker_id,
            batch_size=BATCH_SIZE,
            rate_limit_per_hour=RATE_LIMIT_PER_HOUR,
        )

    if not jobs:
        return 0

    logger.info("slot %s claimed %d job(s)", worker_id, len(jobs))

    tasks = [
        process_single_claimed_job(job, worker_id, http_client)
        for job in jobs
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    return len(jobs)


async def worker_loop(worker_id: str, http_client: httpx.AsyncClient) -> None:
    """Main worker loop fetching and running batches of jobs until shutdown."""
    logger.info(
        "worker slot %s starting (batch_size=%d, failure_rate=%s, poll_interval=%ss)",
        worker_id,
        BATCH_SIZE,
        FAILURE_RATE,
        POLL_INTERVAL_SECONDS,
    )
    while not _shutdown.is_set():
        try:
            processed_count = await process_batch_jobs(worker_id, http_client)
        except Exception:
            logger.exception("unexpected error in worker slot %s", worker_id)
            processed_count = 0

        if processed_count == 0:
            try:
                await asyncio.wait_for(
                    _shutdown.wait(), timeout=POLL_INTERVAL_SECONDS
                )
            except asyncio.TimeoutError:
                pass

    logger.info("worker slot %s shutting down", worker_id)


async def recovery_loop() -> None:
    """Background loop that recovers jobs left in 'processing' by crashed/timed-out workers."""
    logger.info("starting stale job recovery loop")
    while not _shutdown.is_set():
        try:
            async with AsyncSessionLocal() as session:
                repo = JobRepository(session)
                recovered = await repo.recover_stuck_jobs(
                    timeout_seconds=STUCK_JOB_TIMEOUT_SECONDS
                )
                if recovered > 0:
                    logger.warning("[Recovery] Re-queued %d stuck job(s).", recovered)
        except Exception:
            logger.exception("[Recovery] Unexpected error recovering stuck jobs")

        try:
            await asyncio.wait_for(
                _shutdown.wait(), timeout=RECOVERY_INTERVAL_SECONDS
            )
        except asyncio.TimeoutError:
            pass


def _handle_signal(*_args) -> None:
    logger.info("Received shutdown signal. Stopping worker threads...")
    _shutdown.set()


async def main_async() -> None:
    loop = asyncio.get_running_loop()
    concurrency = int(os.environ.get("WORKER_CONCURRENCY", "1"))
    slots = [f"{WORKER_ID}-slot{i}" for i in range(concurrency)]

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    async with httpx.AsyncClient(timeout=5.0) as http_client:
        recovery_task = asyncio.create_task(recovery_loop())

        worker_tasks = [
            asyncio.create_task(worker_loop(slot_id, http_client))
            for slot_id in slots
        ]

        await asyncio.gather(*worker_tasks, return_exceptions=True)

        recovery_task.cancel()
        try:
            await recovery_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main_async())