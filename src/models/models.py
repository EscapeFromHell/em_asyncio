from datetime import datetime, date

import sqlalchemy.orm as so
from sqlalchemy.ext.asyncio import AsyncAttrs


class Base(AsyncAttrs, so.DeclarativeBase):
    pass


class SpimexTradingResults(Base):
    __tablename__ = "spimex_trading_results"
    id: so.Mapped[int] = so.mapped_column(primary_key=True)
    exchange_product_id: so.Mapped[str]
    exchange_product_name: so.Mapped[str]
    oil_id: so.Mapped[str]
    delivery_basis_id: so.Mapped[str]
    delivery_basis_name: so.Mapped[str]
    delivery_type_id: so.Mapped[str]
    volume: so.Mapped[int]
    total: so.Mapped[int]
    count: so.Mapped[int]
    date: so.Mapped[date]
    created_on: so.Mapped[datetime] = so.mapped_column(default=datetime.now)
    updated_on: so.Mapped[datetime] = so.mapped_column(default=datetime.now, onupdate=datetime.now)
