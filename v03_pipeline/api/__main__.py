from aiohttp import web

from v03_pipeline.api.app import init_web_app
from v03_pipeline.lib.logger import get_logger


def run():
    app = init_web_app()
    logger = get_logger(__name__)
    web.run_app(
        app,
        host='0.0.0.0',
        port=5000,
        access_log=logger,
    )


run()
