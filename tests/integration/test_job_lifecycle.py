from datetime import datetime, timedelta, timezone

import pytest

from common.models import Job
from common.repositories.jobs import JobRepository


pytestmark = pytest.mark.integration


def failing_job(*, max_attempts: int = 3) -> Job:
    return Job(
        idempotency_key=f"failure-test-{max_attempts}",
        recipient="failure@example.com",
        channel="email",
        payload={"subject": "Failure"},
        scheduled_for=datetime.now(timezone.utc) - timedelta(minutes=1),
        priority=1,
        max_attempts=max_attempts,
    )


@pytest.mark.asyncio
async def test_retry_uses_exponential_backoff(session_factory):
    job = failing_job()
    async with session_factory() as session:
        session.add(job)
        await session.commit()
        job_id = job.id

    before_first_failure = datetime.now(timezone.utc)
    async with session_factory() as session:
        repository = JobRepository(session)
        first = await repository.mark_failed_or_dead_letter(
            job_id, error="temporary", base_delay_seconds=2
        )

    assert first is not None
    assert first.status == "failed"
    assert first.attempts == 1
    assert first.next_retry_at is not None
    first_delay = (first.next_retry_at - before_first_failure).total_seconds()
    assert 1.5 <= first_delay <= 3.5

    before_second_failure = datetime.now(timezone.utc)
    async with session_factory() as session:
        repository = JobRepository(session)
        second = await repository.mark_failed_or_dead_letter(
            job_id, error="temporary again", base_delay_seconds=2
        )

    assert second is not None
    assert second.status == "failed"
    assert second.attempts == 2
    assert second.next_retry_at is not None
    second_delay = (second.next_retry_at - before_second_failure).total_seconds()
    assert 3.5 <= second_delay <= 5.5


@pytest.mark.asyncio
async def test_retry_cap_moves_job_to_dead_letter_status(session_factory):
    job = failing_job(max_attempts=1)
    async with session_factory() as session:
        session.add(job)
        await session.commit()
        job_id = job.id

    async with session_factory() as session:
        updated = await JobRepository(session).mark_failed_or_dead_letter(
            job_id, error="permanent", base_delay_seconds=2
        )

    assert updated is not None
    assert updated.status == "dead_lettered"
    assert updated.attempts == 1
    assert updated.next_retry_at is None
    assert updated.last_error == "permanent"
