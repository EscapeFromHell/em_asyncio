import os
from typing import Generator
from datetime import datetime, timedelta, date
import asyncio
import pandas as pd
import httpx

from src.models import SpimexTradingResults, Base
from src.config import settings
from src.db import async_session, engine


class SpimexParser:
    """
    Класс для парсинга Spimex.com и извлечения результатов торгов.
    """
    def __init__(self, target_date: date) -> None:
        """
        Инициализирует класс SpimexParser.

        Args:
            target_date (datetime): Целевая дата для извлечения результатов торгов.
        """
        self.target_date: date = target_date
        self.date_list: list = self.prepare_date_list(target_date=self.target_date)

    def __remove_excel(self) -> None:
        """
        Удаляет файл oil_data.xls, если он существует.
        """
        for str_date in self.date_list:
            if os.path.exists(f"{str_date}_oil_data.xls"):
                os.remove(f"{str_date}_oil_data.xls")

    @staticmethod
    async def __add_to_db(objects: list[SpimexTradingResults]) -> None:
        """
        Добавляет список объектов SpimexTradingResult в базу данных.

        Args:
            objects (list[SpimexTradingResult]): Список объектов SpimexTradingResult для добавления.
        """
        async with async_session() as session:
            async with session.begin():
                session.add_all(objects)

    async def __download_spimex_bulletin(self) -> None:
        """
        Загружает бюллетень Spimex.

        Args:
            url (str): URL бюллетеня Spimex.

        Raises:
            Exception: Если произошла ошибка при загрузке бюллетеня.
        """
        try:
            async with httpx.AsyncClient() as client:
                for str_date in self.date_list:
                    url = settings.URL + str_date + "162000.xls"
                    response = await client.get(url=url, timeout=1)
                    if response.status_code == 200:
                        with open(f"{str_date}_oil_data.xls", "wb") as file:
                            file.write(response.content)
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            print(f"Ошибка при скачивании бюллетеня: {e}!")

    def __get_data_from_excel(self) -> Generator[tuple[list, str]]:
        """
        Получает данные из файла Excel.

        Yields:
            Generator: Данные из файла Excel.
        """
        for str_date in self.date_list:
            if os.path.exists(f"{date}_oil_data.xls"):
                df = pd.read_excel(f"{date}_oil_data.xls")
                target = 0
                for index, row in df.iterrows():
                    try:
                        if "Единица измерения: Метрическая тонна" in row.iloc[1]:
                            target = index
                            break
                    except TypeError:
                        pass
                df = df.iloc[target + 2:]
                df = df.iloc[:-2, [1, 2, 3, 4, 5, -1]]
                df = df.replace("-", 0)
                df = df.fillna(0)
                result = df[df.iloc[:, -1].astype(int) > 0]
                for index, row in result.iterrows():
                    yield row.tolist(), str_date

    def __prepare_objects(self) -> list[SpimexTradingResults]:
        """
        Подготавливает список объектов SpimexTradingResult из данных в файле Excel.

        Returns:
            list[SpimexTradingResult]: Список объектов SpimexTradingResult.
        """
        objects = []
        for data, str_date in self.__get_data_from_excel():
            trading_result = SpimexTradingResults(
                exchange_product_id=data[0],
                exchange_product_name=data[1],
                oil_id=data[0][:4],
                delivery_basis_id=data[0][4:7],
                delivery_basis_name=data[2],
                delivery_type_id=data[0][-1],
                volume=int(data[3]),
                total=int(data[4]),
                count=int(data[5]),
                date=datetime.strptime(str_date, "%Y%m%d").date()
            )
            objects.append(trading_result)
        return objects

    @staticmethod
    def prepare_date_list(target_date: date) -> list[str]:
        date_list = []
        current_datetime = datetime.now()
        current_date = current_datetime.date()
        while current_date >= target_date:
            str_date = str(current_date).replace("-", "")
            date_list.append(str_date)
            current_date -= timedelta(days=1)
        return date_list

    async def start_process(self) -> None:
        """
        Начинает процесс парсинга Spimex и добавления результатов торгов в базу данных.
        """
        try:
            print("Выполнение...")
            await self.__download_spimex_bulletin()
            objects = self.__prepare_objects()
            await self.__add_to_db(objects=objects)
            print("Hello")
            self.__remove_excel()
        except Exception as e:
            print(f"Возникла ошибка при выполнении! {e}")
        else:
            print("Выполнение завершено!")

    @staticmethod
    async def create_metadata() -> None:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()


if __name__ == "__main__":
    sp = SpimexParser(date(2024, 4, 1))
    asyncio.run(sp.create_metadata())
    asyncio.run(sp.start_process())
