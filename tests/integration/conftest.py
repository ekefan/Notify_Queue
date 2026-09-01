import os
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine


def _asyncpg_url(url: str) -> str:
    scheme, remainder = url.split("://", 1)
    if scheme.startswith("postgresql"):
        return f"postgresql+asyncpg://{remainder}"
    raise ValueError(f"unexpected PostgreSQL URL: {url}")


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    try:
        from testcontainers.community.postgres import PostgresContainer

        container = PostgresContainer("postgres:16-alpine")
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker/Testcontainers is unavailable: {exc}")

    url = _asyncpg_url(container.get_connection_url())
    previous_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url
    try:
        command.upgrade(Config("alembic.ini"), "head")
        yield url
    finally:
        if previous_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_url
        container.stop()


@pytest_asyncio.fixture
async def integration_engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    # asyncpg connections are bound to the event loop that created them. Pytest may
    # create a separate loop per test, so the pool must not be shared session-wide.
    engine = create_async_engine(postgres_url, pool_size=10)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def clean_database(integration_engine: AsyncEngine):
    async with integration_engine.begin() as connection:
        await connection.execute(text("TRUNCATE TABLE jobs RESTART IDENTITY CASCADE"))
    yield


@pytest.fixture
def session_factory(integration_engine: AsyncEngine):
    return async_sessionmaker(integration_engine, expire_on_commit=False)
