import asyncio
import logging

import aio_pika
from sqlalchemy import func, select

from broker.messages import JobMessage
from broker.topology import WORK_ROUTING_KEY
from common.models import Job, PublishOutbox
from observability.metrics import PUBLISHED, PUBLISH_FAILURES


logger = logging.getLogger("publisher")


async def publish_due_batch(
    session, exchange, *, batch_size: int, publish_concurrency: int = 25
) -> int:
    statement = (
        select(PublishOutbox, Job)
        .join(Job, Job.id == PublishOutbox.job_id)
        .where(
            PublishOutbox.published_at.is_(None),
            PublishOutbox.available_at <= func.now(),
        )
        .order_by(PublishOutbox.available_at, PublishOutbox.created_at)
        .limit(batch_size)
        .with_for_update(of=PublishOutbox, skip_locked=True)
    )
    rows = (await session.execute(statement)).all()

    semaphore = asyncio.Semaphore(publish_concurrency)

    async def publish_one(event, job):
        message = JobMessage(job_id=job.id, outbox_id=event.id)
        async with semaphore:
            await exchange.publish(
                aio_pika.Message(
                    body=message.encode(),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=str(event.id),
                    correlation_id=str(job.id),
                    priority=job.priority,
                ),
                routing_key=WORK_ROUTING_KEY,
                mandatory=True,
            )
        return event, job

    results = await asyncio.gather(
        *(publish_one(event, job) for event, job in rows),
        return_exceptions=True,
    )

    published = 0
    for row, result in zip(rows, results, strict=True):
        event, job = row
        if isinstance(result, BaseException):
            event.attempts += 1
            event.last_error = str(result)
            PUBLISH_FAILURES.inc()
            logger.error("failed to publish outbox event %s: %s", event.id, result)
            continue

        event.attempts += 1
        event.last_error = None
        event.published_at = func.now()
        PUBLISHED.inc()
        published += 1

    await session.commit()
    if rows:
        logger.info(
            "publisher batch complete selected=%s published=%s failed=%s",
            len(rows),
            published,
            len(rows) - published,
        )
    return len(rows)
