from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response, logger, status
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.ext.asyncio import AsyncSession

from common.database import get_session
from common.repositories.jobs import JobRepository
from producer.job.model import (
    JobStatusResp,
    ScheduleJobReq,
    ScheduleJobResp,
    ScheduledJobResp,
    WebhookEvent,
)
from producer.job.service import JobNotFoundError, JobService
from common.database import engine
from observability.metrics import JOBS_BY_STATUS, observe_pool
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app: FastAPI = FastAPI()
logger  = logging.getLogger("producer api")
@app.get("/")
def read_root():
    return {"Hello": "World"}

def get_job_service(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> JobService:
    return JobService(JobRepository(session))


JobServiceDep = Annotated[JobService, Depends(get_job_service)]


@app.post("/jobs", response_model=ScheduleJobResp,
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"description": "Idempotency key already belongs to a job"}},
)
async def schedule_job(
    body: ScheduleJobReq,
    service: JobServiceDep,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
):
    result = await service.schedule(body, idempotency_key=idempotency_key)
    job_response = ScheduledJobResp.model_validate(result.job)
    if result.deduplicated:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "idempotency key already belongs to an existing job",
                "job_id": str(job_response.id),
                "status": job_response.status
            },
        )
    return ScheduleJobResp.model_validate(
        {**job_response.model_dump(), "deduplicated": False}
    )

@app.post("/webhook/receive")
async def handle_job_webhook(event: WebhookEvent):
    logger.info("webhook received: job_id=%s status=%s", event.job_id, event.status)
    return {"received": True, "job_id": str(event.job_id), "status": event.status}

@app.get("/jobs/metrics")
async def job_metrics(service: JobServiceDep):
    return await service.metrics()


@app.get("/metrics", include_in_schema=False)
async def prometheus_metrics(service: JobServiceDep):
    counts = await service.metrics()
    for job_status, count in counts.items():
        JOBS_BY_STATUS.labels(status=job_status).set(count)
    observe_pool(engine.pool)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


# @app.get("/jobs", response_model=list[ScheduledJobResp])
# async def list_jobs(
#     service: JobServiceDep,
#     limit: Annotated[int, Query(ge=1, le=500)] = 100,
#     offset: Annotated[int, Query(ge=0)] = 0,
# ):
#     return await service.list(limit=limit, offset=offset)


# @app.get("/jobs/{job_id}", response_model=ScheduledJobResp)
# async def fetch_job(job_id: UUID, service: JobServiceDep):
#     try:
#         return await service.get(job_id)
#     except JobNotFoundError as exc:
#         raise HTTPException(status_code=404, detail=str(exc)) from exc

@app.get("/jobs/{job_id}/status", response_model=JobStatusResp)
async def fetch_job_status(job_id: UUID, service: JobServiceDep):
    try:
        return await service.get(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
