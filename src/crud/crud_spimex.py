from src.models import SpimexTradingResults


class CRUDSpimex:
    def __init__(self, async_session) -> None:
        self.async_session = async_session

    async def add_to_db(self, objects: list[SpimexTradingResults]) -> None:
        """
        Добавляет список объектов SpimexTradingResult в базу данных.

        Args:
            objects (list[SpimexTradingResult]): Список объектов SpimexTradingResult для добавления.
        """
        async with self.async_session() as session:
            async with session.begin():
                session.add_all(objects)
