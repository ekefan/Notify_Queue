"""Benchmark the real PostgreSQL outbox -> RabbitMQ publisher path.

The target database and RabbitMQ topology must be disposable. This script creates the
schema, inserts synthetic jobs/outbox events, publishes every due event through the
same service used by v2, prints throughput, then deletes the benchmark broker objects.
"""

import argparse
import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from broker.topology import (
    DLQ_QUEUE,
    DLX_EXCHANGE,
    WORK_EXCHANGE,
    WORK_QUEUE,
    connect,
    declare_topology,
)
from common.database import Base
from common.models import Job, PublishOutbox
from publisher.service import publish_due_batch


logging.basicConfig(level=logging.WARNING)


async def seed(session_factory, count: int, insert_batch_size: int) -> None:
    due_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    for offset in range(0, count, insert_batch_size):
        size = min(insert_batch_size, count - offset)
        job_ids = [uuid.uuid4() for _ in range(size)]
        outbox_ids = [uuid.uuid4() for _ in range(size)]
        async with session_factory() as session:
            await session.execute(
                insert(Job),
                [
                    {
                        "id": job_id,
                        "idempotency_key": f"publisher-benchmark-{offset + index}",
                        "recipient": f"benchmark-{offset + index}@example.com",
                        "channel": "push",
                        "payload": {"message": "benchmark"},
                        "scheduled_for": due_at,
                        "priority": (offset + index) % 3,
                        "status": "pending",
                        "attempts": 0,
                        "max_attempts": 5,
                    }
                    for index, job_id in enumerate(job_ids)
                ],
            )
            await session.execute(
                insert(PublishOutbox),
                [
                    {
                        "id": outbox_id,
                        "job_id": job_id,
                        "available_at": due_at,
                        "attempts": 0,
                    }
                    for outbox_id, job_id in zip(outbox_ids, job_ids, strict=True)
                ],
            )
            await session.commit()


async def cleanup_broker(channel) -> None:
    for queue_name in (WORK_QUEUE, DLQ_QUEUE):
        queue = await channel.get_queue(queue_name, ensure=False)
        await queue.delete(if_unused=False, if_empty=False)
    for exchange_name in (WORK_EXCHANGE, DLX_EXCHANGE):
        exchange = await channel.get_exchange(exchange_name, ensure=False)
        await exchange.delete(if_unused=False)


async def benchmark(
    count: int,
    publish_batch_size: int,
    insert_batch_size: int,
    publish_concurrency: int,
) -> None:
    database_url = os.environ["BENCHMARK_DATABASE_URL"]
    engine = create_async_engine(database_url, pool_size=2, max_overflow=0)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)

    await seed(session_factory, count, insert_batch_size)

    connection = await connect()
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange, queue = await declare_topology(channel)
        await queue.purge()

        started = time.perf_counter()
        selected = 0
        while selected < count:
            async with session_factory() as session:
                batch_count = await publish_due_batch(
                    session,
                    exchange,
                    batch_size=publish_batch_size,
                    publish_concurrency=publish_concurrency,
                )
            if batch_count == 0:
                break
            selected += batch_count
        elapsed = time.perf_counter() - started

        async with session_factory() as session:
            confirmed = await session.scalar(
                select(func.count(PublishOutbox.id)).where(
                    PublishOutbox.published_at.is_not(None)
                )
            )

        declaration = await queue.declare()
        queued = declaration.message_count
        rate = confirmed / elapsed if elapsed else 0
        print(f"jobs={count}")
        print(f"confirmed={confirmed}")
        print(f"queued={queued}")
        print(f"elapsed_seconds={elapsed:.3f}")
        print(f"published_per_second={rate:.1f}")
        print(f"batch_size={publish_batch_size}")
        print(f"publisher_concurrency={publish_concurrency}")

        await cleanup_broker(channel)

    await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--insert-batch-size", type=int, default=5_000)
    parser.add_argument("--concurrency", type=int, default=25)
    args = parser.parse_args()
    asyncio.run(
        benchmark(
            args.count,
            args.batch_size,
            args.insert_batch_size,
            args.concurrency,
        )
    )


if __name__ == "__main__":
    main()
