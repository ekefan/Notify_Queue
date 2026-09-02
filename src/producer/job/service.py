from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from common.models import Job, PublishOutbox
from common.repositories.jobs import JobRepository
from producer.job.model import ScheduleJobReq


class JobNotFoundError(Exception):
    pass


@dataclass(frozen=True)
class ScheduleResult:
    job: Job
    deduplicated: bool


class JobService:
    def __init__(self, repository: JobRepository) -> None:
        self.repository = repository

    async def schedule(
        self, request: ScheduleJobReq, *, idempotency_key: str
    ) -> ScheduleResult:
        existing = await self.repository.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return ScheduleResult(job=existing, deduplicated=True)

        job = Job(
            idempotency_key=idempotency_key,
            recipient=request.recipient,
            channel=request.channel.value,
            payload=request.payload,
            scheduled_for=request.send_at,
            priority=request.priority.value,
            status="pending",
        )
    
        try:
            await self.repository.add(job)
            self.repository.session.add(
                PublishOutbox(job_id=job.id, available_at=job.scheduled_for)
            )
            await self.repository.session.commit()
        except IntegrityError:
            await self.repository.session.rollback()
            existing = await self.repository.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            return ScheduleResult(job=existing, deduplicated=True)

        await self.repository.session.refresh(job)
        return ScheduleResult(job=job, deduplicated=False)

    async def get(self, job_id: UUID) -> Job:
        job = await self.repository.get_by_id(job_id)
        if job is None:
            raise JobNotFoundError(f"job {job_id} was not found")
        return job

    async def list(self, *, limit: int, offset: int):
        return await self.repository.list(limit=limit, offset=offset)

    async def metrics(self) -> dict[str, int]:
        return await self.repository.counts_by_status()
