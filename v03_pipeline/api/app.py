from aiohttp import web

from v03_pipeline.lib.tasks import *  # noqa: F403


async def status(_: web.Request) -> web.Response:
    return web.json_response({'success': True})


async def init_web_app():
    app = web.Application()
    app.add_routes(
        [
            web.get('/status', status),
        ],
    )
    return app
