from __future__ import annotations

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from src.config import settings


engine = create_async_engine(url=settings.DATABASE_DSN, echo=True)
async_session = async_sessionmaker(engine, expire_on_commit=False)
