from collections.abc import Sequence
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
        statement = (
            select(Job)
            .where(Job.id == job_id)
            .with_for_update()
        )
        job = await self.session.scalar(statement)
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
        """Executes failure handling atomically in SQL to avoid timezone issues."""
        statement = text(
            """
            UPDATE jobs
            SET 
                attempts = attempts + 1,
                last_error = :error,
                status = CASE 
                    WHEN attempts + 1 >= max_attempts THEN 'dead_lettered'
                    ELSE 'failed'
                END,
                next_retry_at = CASE 
                    WHEN attempts + 1 < max_attempts THEN now() + (:base_delay * (2 ^ attempts) * interval '1 second')
                    ELSE NULL
                END,
                scheduled_for = CASE 
                    WHEN attempts + 1 < max_attempts THEN now() + (:base_delay * (2 ^ attempts) * interval '1 second')
                    ELSE scheduled_for
                END
            WHERE id = :job_id
            RETURNING id
            """
        )
        result = await self.session.execute(
            statement,
            {
                "job_id": job_id,
                "error": error,
                "base_delay": base_delay_seconds,
            },
        )
        row = result.first()
        await self.session.commit()

        if row is None:
            return None

        return await self.get_by_id(job_id)

    async def defer_rate_limited_job(self, job_id: UUID, defer_seconds: int = 300) -> None:
        """Postpones execution when a recipient rate-limit is encountered."""
        statement = text(
            """
            UPDATE jobs
            SET status = 'pending',
                scheduled_for = now() + (:defer_seconds * interval '1 second'),
                claimed_by = NULL,
                claimed_at = NULL
            WHERE id = :job_id
            """
        )
        await self.session.execute(statement, {"job_id": job_id, "defer_seconds": defer_seconds})
        await self.session.commit()

    async def recover_stuck_jobs(self, timeout_seconds: int = 300) -> int:
        """
        Resets jobs stuck in 'processing' state if the worker timed out or crashed.
        Moves them back to 'pending' so another worker can pick them up.
        """
        statement = text(
            """
            UPDATE jobs
            SET status = 'pending',
                claimed_by = NULL,
                claimed_at = NULL
            WHERE status = 'processing'
              AND claimed_at < now() - (:timeout_seconds * interval '1 second')
            """
        )
        result = await self.session.execute(statement, {"timeout_seconds": timeout_seconds})
        recovered_count = result.rowcount
        await self.session.commit()
        return recovered_count

    async def claim_next_jobs(
        self,
        worker_id: str,
        *,
        batch_size: int = 10,
        rate_limit_per_hour: int = 10,
    ) -> Sequence[Job]:
        """
        Claims up to `batch_size` jobs in a single lock query, filters out rate-limited 
        recipients, defers any rate-limited jobs, and returns valid jobs for processing.
        """
        claim_statement = text(
            """
            WITH target_jobs AS (
                SELECT id 
                FROM jobs
                WHERE status IN ('pending', 'failed')
                  AND scheduled_for <= now()
                ORDER BY (priority + EXTRACT(EPOCH FROM (now() - scheduled_for)) / 60) DESC, scheduled_for ASC
                FOR UPDATE SKIP LOCKED
                LIMIT :batch_size
            )
            UPDATE jobs j
            SET status = 'processing',
                claimed_by = :worker_id,
                claimed_at = now()
            FROM target_jobs tj
            WHERE j.id = tj.id
            RETURNING j.id, j.recipient
            """
        )
        result = await self.session.execute(
            claim_statement, {"worker_id": worker_id, "batch_size": batch_size}
        )
        claimed_rows = result.all()

        if not claimed_rows:
            await self.session.commit()
            return []

        recipients = list({row.recipient for row in claimed_rows})
        rate_check_statement = text(
            """
            SELECT recipient, COUNT(*) as sent_count
            FROM jobs
            WHERE recipient = ANY(:recipients)
              AND status = 'sent'
              AND sent_at >= now() - interval '1 hour'
            GROUP BY recipient
            """
        )
        rate_result = await self.session.execute(
            rate_check_statement, {"recipients": recipients}
        )
        sent_counts = {row.recipient: row.sent_count for row in rate_result.all()}

        # Step 3: Separate executable jobs from rate-limited jobs
        valid_job_ids: list[UUID] = []
        rate_limited_job_ids: list[UUID] = []

        for row in claimed_rows:
            if sent_counts.get(row.recipient, 0) >= rate_limit_per_hour:
                rate_limited_job_ids.append(row.id)
            else:
                valid_job_ids.append(row.id)
                # Increment in-memory counter to prevent over-allocating in this same batch
                sent_counts[row.recipient] = sent_counts.get(row.recipient, 0) + 1

        # Step 4: Defer rate-limited jobs by 5 minutes
        if rate_limited_job_ids:
            defer_statement = text(
                """
                UPDATE jobs
                SET status = 'pending',
                    scheduled_for = now() + interval '5 minutes',
                    claimed_by = NULL,
                    claimed_at = NULL
                WHERE id = ANY(:job_ids)
                """
            )
            await self.session.execute(defer_statement, {"job_ids": rate_limited_job_ids})

        await self.session.commit()

        if not valid_job_ids:
            return []

        # Step 5: Fetch and return full ORM entities for valid jobs
        jobs_statement = select(Job).where(Job.id.in_(valid_job_ids))
        jobs_result = await self.session.scalars(jobs_statement)
        return jobs_result.all()