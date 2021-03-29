from aiohttp.web_urldispatcher import View
from asyncpgsa import PG


class BaseView(View):
    """
    base entity for all handlers
    """
    URL_PATH: str

    @property
    def pg(self) -> PG:
        return self.request.app['pg']
