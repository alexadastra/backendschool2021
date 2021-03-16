from aiohttp.web import View
from aiohttp.web_response import json_response


class SomeView(View):
    URL_PATH = r'/hello'

    async def get(self):
        return json_response({'hello': 'world'})
