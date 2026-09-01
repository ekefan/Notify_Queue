from fastapi import FastAPI, status
from job.model import ScheduledJobResp,  ScheduleJobReq

app: FastAPI = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/jobs", response_model=ScheduledJobResp, status_code=status.HTTP_201_CREATED)
async def schedule_job(body:  ScheduleJobReq):
    return {"job_stat": "scheduled"}

@app.post("/webhook/receive")
async def handle_job_webhook():
    return {"job_stat": "handling webhook"}

@app.get("/jobs/metrics")
async def job_metrics():
    return {"job_stat": "fetching job metrics"}

@app.get("/jobs/{job_id}/status")
async def fetch_job_status(job_id: str):
    return {"job_stat": "fetching job status"}