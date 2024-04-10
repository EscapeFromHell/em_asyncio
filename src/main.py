import os
from datetime import datetime, timedelta, date
import asyncio

from src.clients import SpimexClient
from src.crud import CRUDSpimex
from src.db import async_session, create_metadata
from src.servises import TradingResultsHandler


class SpimexParser:
    """
    Класс для извлечения результатов торгов с сайта Spimex.com и записи данных в БД.
    """
    def __init__(self, target_date: date) -> None:
        """
        Инициализирует класс SpimexParser.

        Args:
            target_date (date): Целевая дата для извлечения результатов торгов.
        """
        self.target_date: date = target_date
        self.date_list: list = self.__prepare_date_list()
        self.spimex_client = SpimexClient(date_list=self.date_list)
        self.trading_results_handler = TradingResultsHandler(date_list=self.date_list)
        self.crud = CRUDSpimex(async_session=async_session)

    def __remove_excel(self) -> None:
        """
        Удаляет файл oil_data.xls, если он существует.
        """
        for str_date in self.date_list:
            if os.path.exists(f"{str_date}_oil_data.xls"):
                os.remove(f"{str_date}_oil_data.xls")

    def __prepare_date_list(self) -> list[str]:
        """
        Подготавливает список дат между текущей датой и целевой датой.

        Returns:
            list[str]: Список дат между текущей датой и целевой датой.
        """
        date_list = []
        current_datetime = datetime.now()
        current_date = current_datetime.date()
        while current_date >= self.target_date:
            str_date = str(current_date).replace("-", "")
            date_list.append(str_date)
            current_date -= timedelta(days=1)
        return date_list

    async def start_process(self) -> None:
        """
        Начинает процесс скачивания результатов торгов с сайта Spimex и добавления их в базу данных.
        """
        try:
            print("Выполнение...")
            await self.spimex_client.download_spimex_bulletin()
            objects = self.trading_results_handler.prepare_objects()
            await self.crud.add_to_db(objects=objects)
            self.__remove_excel()
        except Exception as e:
            print(f"Возникла ошибка при выполнении! {e}")
        else:
            print("Выполнение завершено!")


if __name__ == "__main__":
    sp = SpimexParser(date(2023, 1, 1))
    asyncio.run(create_metadata())
    asyncio.run(sp.start_process())
