import os

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractRobustConnection


RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost/")
WORK_EXCHANGE = os.environ.get("RABBITMQ_WORK_EXCHANGE", "notify.jobs")
WORK_QUEUE = os.environ.get("RABBITMQ_WORK_QUEUE", "notify.jobs.ready")
WORK_ROUTING_KEY = os.environ.get("RABBITMQ_WORK_ROUTING_KEY", "notify.ready")
DLX_EXCHANGE = os.environ.get("RABBITMQ_DLX_EXCHANGE", "notify.jobs.dlx")
DLQ_QUEUE = os.environ.get("RABBITMQ_DLQ_QUEUE", "notify.jobs.dead")
DLQ_ROUTING_KEY = os.environ.get("RABBITMQ_DLQ_ROUTING_KEY", "notify.dead")
MAX_PRIORITY = int(os.environ.get("RABBITMQ_MAX_PRIORITY", "2"))


async def connect() -> AbstractRobustConnection:
    return await aio_pika.connect_robust(RABBITMQ_URL)


async def declare_topology(channel: AbstractChannel):
    work_exchange = await channel.declare_exchange(
        WORK_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )
    dlx = await channel.declare_exchange(
        DLX_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )
    queue = await channel.declare_queue(
        WORK_QUEUE,
        durable=True,
        arguments={
            "x-max-priority": MAX_PRIORITY,
            "x-dead-letter-exchange": DLX_EXCHANGE,
            "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
        },
    )
    await queue.bind(work_exchange, WORK_ROUTING_KEY)
    dead_queue = await channel.declare_queue(DLQ_QUEUE, durable=True)
    await dead_queue.bind(dlx, DLQ_ROUTING_KEY)
    return work_exchange, queue
