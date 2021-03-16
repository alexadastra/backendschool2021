from aiohttp.web_exceptions import HTTPNotFound
from sqlalchemy import exists, select

from store.api.handlers.base.base import BaseView
from store.db.schema import couriers_imports_table, orders_imports_table


class BaseImportView(BaseView):
    @property
    def import_id(self):
        return int(self.request.match_info.get('import_id'))

    async def check_import_exists(self):
        pass


class BaseCourierImportView(BaseImportView):
    async def check_import_exists(self):
        query = select([
            exists().where(couriers_imports_table.c.import_id == self.import_id)
        ])
        if not await self.pg.fetchval(query):
            raise HTTPNotFound()


class BaseOrderImportView(BaseImportView):
    async def check_import_exists(self):
        query = select([
            exists().where(orders_imports_table.c.import_id == self.import_id)
        ])
        if not await self.pg.fetchval(query):
            raise HTTPNotFound()
