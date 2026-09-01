import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from common.models import Job
from common.repositories.jobs import JobRepository


pytestmark = pytest.mark.integration


def make_job(
    number: int,
    *,
    priority: int = 1,
    scheduled_for: datetime | None = None,
    recipient: str | None = None,
    max_attempts: int = 5,
) -> Job:
    return Job(
        idempotency_key=f"worker-concurrency-{number}",
        recipient=recipient or f"person-{number}@example.com",
        channel="email",
        payload={"subject": f"Job {number}"},
        scheduled_for=scheduled_for
        or datetime.now(timezone.utc) - timedelta(minutes=1),
        priority=priority,
        max_attempts=max_attempts,
    )


async def claim(session_factory, worker_id: str, *, batch_size: int = 1):
    async with session_factory() as session:
        return await JobRepository(session).claim_next_jobs(
            worker_id,
            batch_size=batch_size,
            rate_limit_per_hour=100,
        )


@pytest.mark.asyncio
async def test_only_one_concurrent_worker_claims_a_single_job(session_factory):
    async with session_factory() as session:
        session.add(make_job(1))
        await session.commit()

    claims = await asyncio.gather(
        *(claim(session_factory, f"worker-{number}") for number in range(12))
    )
    claimed_jobs = [job for batch in claims for job in batch]

    assert len(claimed_jobs) == 1
    assert len({job.id for job in claimed_jobs}) == 1

    async with session_factory() as session:
        stored = await session.get(Job, claimed_jobs[0].id)

    assert stored is not None
    assert stored.status == "processing"
    assert stored.claimed_by is not None


@pytest.mark.asyncio
async def test_concurrent_batch_claims_never_overlap(session_factory):
    async with session_factory() as session:
        session.add_all(make_job(number) for number in range(40))
        await session.commit()

    claims = await asyncio.gather(
        *(
            claim(session_factory, f"batch-worker-{number}", batch_size=10)
            for number in range(8)
        )
    )
    claimed_ids = [job.id for batch in claims for job in batch]

    assert len(claimed_ids) == 40
    assert len(set(claimed_ids)) == 40


@pytest.mark.asyncio
async def test_high_priority_job_is_claimed_first_when_jobs_are_equally_due(
    session_factory,
):
    due_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    async with session_factory() as session:
        session.add_all(
            [
                make_job(1, priority=0, scheduled_for=due_at),
                make_job(2, priority=1, scheduled_for=due_at),
                make_job(3, priority=2, scheduled_for=due_at),
            ]
        )
        await session.commit()

    claimed = await claim(session_factory, "priority-worker", batch_size=1)

    assert len(claimed) == 1
    assert claimed[0].priority == 2


@pytest.mark.asyncio
async def test_future_job_cannot_be_claimed(session_factory):
    async with session_factory() as session:
        session.add(
            make_job(
                1,
                scheduled_for=datetime.now(timezone.utc) + timedelta(hours=1),
            )
        )
        await session.commit()

    claimed = await claim(session_factory, "early-worker")

    assert claimed == []


@pytest.mark.asyncio
async def test_rate_limit_defers_excess_jobs_in_one_claimed_batch(session_factory):
    recipient = "rate-limited@example.com"
    async with session_factory() as session:
        session.add_all(
            make_job(number, recipient=recipient) for number in range(5)
        )
        await session.commit()

    async with session_factory() as session:
        claimed = await JobRepository(session).claim_next_jobs(
            "rate-limit-worker",
            batch_size=5,
            rate_limit_per_hour=2,
        )

    assert len(claimed) == 2

    async with session_factory() as session:
        jobs = (await session.scalars(select(Job))).all()

    assert sum(job.status == "processing" for job in jobs) == 2
    assert sum(job.status == "pending" for job in jobs) == 3
    assert all(
        job.scheduled_for > datetime.now(timezone.utc)
        for job in jobs
        if job.status == "pending"
    )


@pytest.mark.asyncio
async def test_expired_processing_job_is_recovered(session_factory):
    stale_claim = datetime.now(timezone.utc) - timedelta(minutes=10)
    job = make_job(1)
    job.status = "processing"
    job.claimed_by = "crashed-worker"
    job.claimed_at = stale_claim

    async with session_factory() as session:
        session.add(job)
        await session.commit()
        job_id = job.id

    async with session_factory() as session:
        recovered = await JobRepository(session).recover_stuck_jobs(
            timeout_seconds=300
        )

    assert recovered == 1

    async with session_factory() as session:
        stored = await session.get(Job, job_id)

    assert stored is not None
    assert stored.status == "pending"
    assert stored.claimed_by is None
    assert stored.claimed_at is None
