from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from common.models import Job


class JobRepository:
    """Shared persistence operations for Jobs."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def add(self, job: Job) -> Job:
        self.session.add(job)
        await self.session.flush()
        return job

    async def get_by_id(self, job_id: UUID) -> Job | None:
        return await self.session.get(Job, job_id)

    async def get_by_idempotency_key(self, key: str) -> Job | None:
        statement = select(Job).where(Job.idempotency_key == key)
        return await self.session.scalar(statement)

    async def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[Job]:
        statement = (
            select(Job)
            .order_by(Job.created_at.desc(), Job.id.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self.session.scalars(statement)
        return result.all()

    async def counts_by_status(self) -> dict[str, int]:
        statement = select(Job.status, func.count(Job.id)).group_by(Job.status)
        rows = (await self.session.execute(statement)).all()
        counts = {
            "pending": 0,
            "processing": 0,
            "sent": 0,
            "failed": 0,
            "dead_lettered": 0,
        }
        counts.update({job_status: count for job_status, count in rows})
        return counts
