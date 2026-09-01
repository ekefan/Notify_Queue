from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from dotenv import load_dotenv

load_dotenv()
import os

DATABASE_URL = os.environ["DATABASE_URL"]
DATABASE_POOL_SIZE = int(os.environ["DATABASE_POOL_SIZE"])

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=DATABASE_POOL_SIZE)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass


async def get_session():
    async with AsyncSessionLocal() as session:
        yield session