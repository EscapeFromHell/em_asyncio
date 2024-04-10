import os
from typing import Generator
from datetime import datetime

import pandas as pd

from src.models import SpimexTradingResults


class TradingResultsHandler:
    def __init__(self, date_list: list[str]) -> None:
        self.date_list = date_list

    def __get_data_from_excel(self) -> Generator[tuple[list, str], None, None]:
        """
        Получает данные из файла Excel.

        Yields:
            Generator: Данные из файла Excel и дата биллютеня.
        """
        for date in self.date_list:
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
                    yield row.tolist(), date

    def prepare_objects(self) -> list[SpimexTradingResults]:
        """
        Подготавливает список объектов SpimexTradingResult из данных в файле Excel.

        Returns:
            list[SpimexTradingResult]: Список объектов SpimexTradingResult.
        """
        objects = []
        for data, date in self.__get_data_from_excel():
            trading_result = SpimexTradingResults(
                exchange_product_id=str(data[0]),
                exchange_product_name=str(data[1]),
                oil_id=str(data[0][:4]),
                delivery_basis_id=str(data[0][4:7]),
                delivery_basis_name=str(data[2]),
                delivery_type_id=str(data[0][-1]),
                volume=str(data[3]),
                total=str(data[4]),
                count=str(data[5]),
                date=datetime.strptime(date, "%Y%m%d").date()
            )
            objects.append(trading_result)
        return objects
