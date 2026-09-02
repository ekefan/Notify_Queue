import asyncio
import logging
import os
import signal

from prometheus_client import start_http_server

from broker.topology import connect, declare_topology
from common.database import AsyncSessionLocal, engine
from observability.metrics import observe_pool
from publisher.service import publish_due_batch


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("publisher")

POLL_SECONDS = float(os.environ.get("PUBLISHER_POLL_SECONDS", "0.25"))
BATCH_SIZE = int(os.environ.get("PUBLISHER_BATCH_SIZE", "100"))
PUBLISH_CONCURRENCY = int(os.environ.get("PUBLISHER_CONCURRENCY", "25"))
METRICS_PORT = int(os.environ.get("PUBLISHER_METRICS_PORT", "9101"))

shutdown = asyncio.Event()


async def main_async() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    start_http_server(METRICS_PORT)
    connection = await connect()
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange, _ = await declare_topology(channel)
        logger.info(
            "publisher started (batch=%s concurrency=%s poll=%ss)",
            BATCH_SIZE,
            PUBLISH_CONCURRENCY,
            POLL_SECONDS,
        )

        while not shutdown.is_set():
            observe_pool(engine.pool)
            async with AsyncSessionLocal() as session:
                published = await publish_due_batch(
                    session,
                    exchange,
                    batch_size=BATCH_SIZE,
                    publish_concurrency=PUBLISH_CONCURRENCY,
                )
            if published == 0:
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=POLL_SECONDS)
                except asyncio.TimeoutError:
                    pass


if __name__ == "__main__":
    asyncio.run(main_async())
