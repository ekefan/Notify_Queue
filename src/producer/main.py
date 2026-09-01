from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from common.database import get_session
from producer.job.model import (
    JobStatusResp,
    ScheduleJobReq,
    ScheduleJobResp,
    ScheduledJobResp,
)
from producer.job.repository import JobRepository
from producer.job.service import JobNotFoundError, JobService

app: FastAPI = FastAPI()

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
                "job_id": job_response.id,
                "status": job_response.status
            },
        )
    return ScheduleJobResp.model_validate(
        {**job_response.model_dump(), "deduplicated": False}
    )

@app.post("/webhook/receive")
async def handle_job_webhook():
    return {"job_stat": "handling webhook"}

@app.get("/jobs/metrics")
async def job_metrics(service: JobServiceDep):
    return await service.metrics()


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
