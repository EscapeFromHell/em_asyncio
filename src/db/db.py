from __future__ import annotations

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from src.config import settings
from src.models import Base


engine = create_async_engine(url=settings.DATABASE_DSN, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)


async def create_metadata() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
