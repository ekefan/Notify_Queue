import asyncio
import logging
import os
import signal
import uuid

import httpx
from aio_pika.abc import AbstractIncomingMessage
from prometheus_client import start_http_server

from broker.messages import JobMessage
from broker.topology import connect, declare_topology
from common.database import engine
from consumer_v2.handler import handle_job
from observability.metrics import REDELIVERIES, observe_pool


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer_v2")

WORKER_ID = f"rabbit-worker-{uuid.uuid4().hex[:8]}"
PREFETCH = int(os.environ.get("RABBITMQ_PREFETCH", "20"))
METRICS_PORT = int(os.environ.get("WORKER_METRICS_PORT", "9102"))
shutdown = asyncio.Event()


async def main_async() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    start_http_server(METRICS_PORT)
    connection = await connect()
    async with connection, httpx.AsyncClient(timeout=5.0) as http_client:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=PREFETCH)
        _, queue = await declare_topology(channel)

        async def consume(incoming: AbstractIncomingMessage) -> None:
            if incoming.redelivered:
                REDELIVERIES.inc()
            try:
                payload = JobMessage.decode(incoming.body)
                observe_pool(engine.pool)
                await handle_job(payload, WORKER_ID, http_client)
            except Exception:
                logger.exception("message handling failed; returning it to RabbitMQ")
                await incoming.nack(requeue=True)
                return
            await incoming.ack()

        consumer_tag = await queue.consume(consume, no_ack=False)
        logger.info("RabbitMQ worker %s started (prefetch=%s)", WORKER_ID, PREFETCH)
        await shutdown.wait()
        await queue.cancel(consumer_tag)


if __name__ == "__main__":
    asyncio.run(main_async())
