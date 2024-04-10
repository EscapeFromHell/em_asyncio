import httpx

from src.config import settings


class SpimexClient:
    def __init__(self, date_list: list[str]) -> None:
        self.date_list = date_list

    async def download_spimex_bulletin(self) -> None:
        """
        Загружает бюллетень Spimex.

        Raises:
            Exception: Если произошла ошибка при загрузке бюллетеня.
        """
        try:
            async with httpx.AsyncClient() as client:
                for date in self.date_list:
                    url = settings.URL + date + "162000.xls"
                    response = await client.get(url=url, timeout=3)
                    if response.status_code == 200:
                        with open(f"{date}_oil_data.xls", "wb") as file:
                            file.write(response.content)
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            print(f"Ошибка при скачивании бюллетеня: {e}!")
