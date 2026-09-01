import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select

from common.database import get_session
from common.models import Job
from producer.main import app


pytestmark = pytest.mark.integration


def payload():
    return {
        "recipient": "person@example.com",
        "channel": "email",
        "payload": {"subject": "Hello"},
        "scheduled_for": datetime(2026, 9, 2, 10, tzinfo=timezone.utc).isoformat(),
        "priority": 2,
    }


@pytest.fixture
async def client(session_factory):
    async def test_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = test_session
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as http_client:
        yield http_client
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_schedule_status_and_metrics(client):
    response = await client.post(
        "/jobs", json=payload(), headers={"Idempotency-Key": "api-test"}
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert body["deduplicated"] is False

    status_response = await client.get(f"/jobs/{body['id']}/status")
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "pending"

    metrics_response = await client.get("/jobs/metrics")
    assert metrics_response.status_code == 200
    assert metrics_response.json()["pending"] == 1


@pytest.mark.asyncio
async def test_duplicate_idempotency_key_returns_existing_job_reference(client):
    headers = {"Idempotency-Key": "duplicate-test"}
    first = await client.post("/jobs", json=payload(), headers=headers)
    second = await client.post("/jobs", json=payload(), headers=headers)

    assert first.status_code == 202
    assert second.status_code == 409
    assert second.json()["detail"]["job_id"] == first.json()["id"]
    assert "payload" not in second.json()["detail"]


@pytest.mark.asyncio
async def test_missing_idempotency_key_is_rejected(client):
    response = await client.post("/jobs", json=payload())
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_unknown_job_status_returns_404(client):
    response = await client.get(f"/jobs/{uuid4()}/status")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_concurrent_duplicate_submissions_create_one_job(client, session_factory):
    async def submit():
        return await client.post(
            "/jobs",
            json=payload(),
            headers={"Idempotency-Key": "concurrent-test"},
        )

    responses = await asyncio.gather(*(submit() for _ in range(20)))

    assert [response.status_code for response in responses].count(202) == 1
    assert [response.status_code for response in responses].count(409) == 19
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count(Job.id)).where(
                Job.idempotency_key == "concurrent-test"
            )
        )
    assert count == 1
