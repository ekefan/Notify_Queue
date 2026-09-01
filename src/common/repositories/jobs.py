from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import func, select, text
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
    async def mark_sent(self, job_id: UUID) -> Job | None:
        job = await self.get_by_id(job_id)
        if job is None:
            return None
        job.status = "sent"
        job.sent_at = func.now()
        await self.session.commit()
        await self.session.refresh(job)
        return job
    async def mark_failed_or_dead_letter(
        self, job_id: UUID, *, error: str, base_delay_seconds: int = 30
    ) -> Job | None:
        job = await self.get_by_id(job_id)
        if job is None:
            return None

        job.attempts += 1
        job.last_error = error

        if job.attempts >= job.max_attempts:
            job.status = "dead_lettered"
        else:
            delay = base_delay_seconds * (2 ** (job.attempts - 1))
            next_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
            job.status = "failed"
            job.next_retry_at = next_time
            job.scheduled_for = next_time

        await self.session.commit()
        await self.session.refresh(job)
        return job

    async def claim_next_job(self, worker_id: str) -> Job | None:
        """
        Atomically claim the single highest-priority, earliest-due pending job
        for this worker. Uses SELECT ... FOR UPDATE SKIP LOCKED so concurrent
        workers never block on or double-claim the same row.
        """
        statement = text(
            """
            UPDATE jobs
            SET status = 'processing',
                claimed_by = :worker_id,
                claimed_at = now()
            WHERE id = (
                SELECT id FROM jobs
                WHERE status IN ('pending', 'failed')
                  AND scheduled_for <= now()
                ORDER BY (priority + EXTRACT(EPOCH FROM (now() - scheduled_for)) / 60) DESC, scheduled_for ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id
            """
        )
        result = await self.session.execute(statement, {"worker_id": worker_id})
        row = result.first()
        await self.session.commit()

        if row is None:
            return None

        return await self.get_by_id(row.id)