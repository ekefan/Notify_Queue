import logging
import os
from datetime import datetime, timezone
from time import monotonic

import httpx
from sqlalchemy import func, select, text

from broker.messages import JobMessage
from common.database import AsyncSessionLocal
from common.models import Job, PublishOutbox
from common.repositories.jobs import JobRepository
from consumer_v2.sender import mock_send
from observability.metrics import (
    DEAD_LETTERS,
    DELIVERIES,
    INFLIGHT,
    PROCESSING_SECONDS,
    QUEUE_WAIT_SECONDS,
    RETRIES,
)


logger = logging.getLogger("consumer_v2")
BASE_RETRY_DELAY_SECONDS = int(os.environ.get("BASE_RETRY_DELAY_SECONDS", "2"))
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "http://localhost:8000/webhook/receive")


async def claim_job(job_id, worker_id: str) -> Job | None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(
                """
                UPDATE jobs
                SET status = 'processing', claimed_by = :worker_id, claimed_at = now()
                WHERE id = :job_id
                  AND status IN ('pending', 'failed')
                  AND scheduled_for <= now()
                RETURNING id
                """
            ),
            {"job_id": job_id, "worker_id": worker_id},
        )
        row = result.first()
        await session.commit()
        if row is None:
            return None
        return await session.get(Job, job_id)


async def send_webhook(client: httpx.AsyncClient, job_id, status: str) -> None:
    try:
        response = await client.post(
            WEBHOOK_URL, json={"job_id": str(job_id), "status": status}
        )
        response.raise_for_status()
    except httpx.HTTPError:
        logger.exception("webhook failed for job %s status=%s", job_id, status)


async def handle_job(
    message: JobMessage, worker_id: str, http_client: httpx.AsyncClient
) -> bool:
    job = await claim_job(message.job_id, worker_id)
    if job is None:
        logger.info("ignoring duplicate or no-longer-due job message %s", message.job_id)
        return True

    now = datetime.now(timezone.utc)
    QUEUE_WAIT_SECONDS.observe(max((now - job.scheduled_for).total_seconds(), 0))
    started = monotonic()
    INFLIGHT.inc()
    try:
        success = await mock_send()
        async with AsyncSessionLocal() as session:
            repository = JobRepository(session)
            if success:
                updated = await repository.mark_sent(job.id)
                status = "sent"
            else:
                updated = await repository.mark_failed_or_dead_letter(
                    job.id,
                    error="Simulated send failure",
                    base_delay_seconds=BASE_RETRY_DELAY_SECONDS,
                )
                status = updated.status if updated else "failed"
                if status == "failed" and updated is not None:
                    session.add(
                        PublishOutbox(
                            job_id=updated.id,
                            available_at=updated.scheduled_for,
                        )
                    )
                    await session.commit()
                    RETRIES.inc()
                elif status == "dead_lettered":
                    DEAD_LETTERS.inc()

        DELIVERIES.labels(status=status, channel=job.channel).inc()
        await send_webhook(http_client, job.id, status)
        logger.info("job %s completed with status=%s", job.id, status)
        return True
    finally:
        INFLIGHT.dec()
        PROCESSING_SECONDS.observe(monotonic() - started)
