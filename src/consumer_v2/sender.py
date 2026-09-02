import asyncio
import os
import random


FAILURE_RATE = float(os.environ.get("MOCK_TRANSIENT_FAILURE_RATE", "0.2"))
MIN_LATENCY_SECONDS = int(os.environ.get("MOCK_MIN_LATENCY_MS", "20")) / 1000
MAX_LATENCY_SECONDS = int(os.environ.get("MOCK_MAX_LATENCY_MS", "100")) / 1000
LONG_PROCESSING_RATE = float(os.environ.get("MOCK_LONG_PROCESSING_RATE", "0.2"))
LONG_PROCESSING_MAX_SECONDS = float(
    os.environ.get("MOCK_LONG_PROCESSING_MAX_SECONDS", "3.0")
)


async def mock_send() -> bool:
    if random.random() < LONG_PROCESSING_RATE:
        latency = random.uniform(MAX_LATENCY_SECONDS, LONG_PROCESSING_MAX_SECONDS)
    else:
        latency = random.uniform(MIN_LATENCY_SECONDS, MAX_LATENCY_SECONDS)
    await asyncio.sleep(latency)
    return random.random() >= FAILURE_RATE
