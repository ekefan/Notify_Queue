from datetime import datetime, timezone

import pytest

from common.models import Job
from common.repositories.jobs import JobRepository


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_repository_adds_and_fetches_job(session_factory):
    async with session_factory() as session:
        repository = JobRepository(session)
        job = Job(
            idempotency_key="repository-test",
            recipient="person@example.com",
            channel="email",
            payload={"subject": "Hello"},
            scheduled_for=datetime(2026, 9, 2, 10, tzinfo=timezone.utc),
            priority=2,
        )
        await repository.add(job)
        await session.commit()
        job_id = job.id

    async with session_factory() as session:
        repository = JobRepository(session)
        stored = await repository.get_by_id(job_id)
        counts = await repository.counts_by_status()

    assert stored is not None
    assert stored.recipient == "person@example.com"
    assert stored.status == "pending"
    assert counts["pending"] == 1
    assert counts["sent"] == 0
