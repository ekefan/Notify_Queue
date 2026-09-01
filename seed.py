"""
Seed the database with sample pending jobs for local testing/demo.

Usage:
    uv run python seed.py
    uv run python seed.py --count 50
"""
import argparse
import asyncio
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from common.database import AsyncSessionLocal
from common.models import Job

CHANNELS = ["email", "sms", "push"]
PRIORITIES = [1, 1, 1, 5, 5, 10]


def make_job(index: int) -> Job:
    channel = random.choice(CHANNELS)
    priority = random.choice(PRIORITIES)

    offset_seconds = random.randint(-300, 300)
    scheduled_for = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)

    recipient = {
        "email": f"user{index}@example.com",
        "sms": f"+1555000{index:04d}",
        "push": f"device-token-{uuid.uuid4().hex[:12]}",
    }[channel]

    payload = {
        "email": {"subject": "Test notification", "body": f"Hello, this is job {index}"},
        "sms": {"message": f"Test SMS for job {index}"},
        "push": {"title": "Update", "body": f"Push payload {index}"},
    }[channel]

    return Job(
        idempotency_key=f"seed-{uuid.uuid4().hex}",
        recipient=recipient,
        channel=channel,
        payload=payload,
        scheduled_for=scheduled_for,
        priority=priority,
        status="pending",
    )


async def seed(count: int) -> None:
    async with AsyncSessionLocal() as session:
        jobs = [make_job(i) for i in range(count)]
        session.add_all(jobs)
        await session.commit()

    print(f"Seeded {count} jobs.")
    by_priority = {}
    for j in jobs:
        by_priority[j.priority] = by_priority.get(j.priority, 0) + 1
    print("By priority:", dict(sorted(by_priority.items(), reverse=True)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed sample jobs")
    parser.add_argument("--count", type=int, default=25)
    args = parser.parse_args()
    asyncio.run(seed(args.count))


if __name__ == "__main__":
    main()