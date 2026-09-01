import os


# Application modules create the engine at import time. Unit tests never connect to
# this URL; integration tests replace it with their Testcontainers URL.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://notify_queue:notify_queue@127.0.0.1:5432/notify_queue",
)
os.environ.setdefault("DATABASE_POOL_SIZE", "5")
