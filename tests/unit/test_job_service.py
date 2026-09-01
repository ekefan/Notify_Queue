from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from common.models import Job
from producer.job.model import ScheduleJobReq
from producer.job.service import JobNotFoundError, JobService


def request() -> ScheduleJobReq:
    return ScheduleJobReq(
        recipient="person@example.com",
        channel="email",
        payload={"subject": "Welcome"},
        scheduled_for=datetime(2026, 9, 2, 10, tzinfo=timezone.utc),
        priority=2,
    )


def repository_mock() -> Mock:
    repository = Mock()
    repository.get_by_idempotency_key = AsyncMock(return_value=None)
    repository.get_by_id = AsyncMock(return_value=None)
    repository.add = AsyncMock(side_effect=lambda job: job)
    repository.counts_by_status = AsyncMock()
    repository.session = Mock()
    repository.session.commit = AsyncMock()
    repository.session.rollback = AsyncMock()
    repository.session.refresh = AsyncMock()
    return repository


@pytest.mark.asyncio
async def test_schedule_creates_pending_job():
    repository = repository_mock()
    service = JobService(repository)

    result = await service.schedule(request(), idempotency_key="request-1")

    assert result.deduplicated is False
    assert result.job.idempotency_key == "request-1"
    assert result.job.recipient == "person@example.com"
    assert result.job.status == "pending"
    repository.add.assert_awaited_once_with(result.job)
    repository.session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_schedule_returns_existing_job_for_duplicate_key():
    repository = repository_mock()
    existing = Job(
        id=uuid4(),
        idempotency_key="request-1",
        recipient="person@example.com",
        channel="email",
        payload={"subject": "Welcome"},
        scheduled_for=datetime(2026, 9, 2, 10, tzinfo=timezone.utc),
        priority=2,
    )
    repository.get_by_idempotency_key.return_value = existing
    service = JobService(repository)

    result = await service.schedule(request(), idempotency_key="request-1")

    assert result.job is existing
    assert result.deduplicated is True
    repository.add.assert_not_awaited()
    repository.session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_missing_job_raises_domain_error():
    service = JobService(repository_mock())

    with pytest.raises(JobNotFoundError):
        await service.get(uuid4())
