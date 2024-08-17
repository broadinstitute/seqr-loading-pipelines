import json
import os
import traceback

import aiofiles
from aiohttp import web

from v03_pipeline.api.model import LoadingPipelineRequest
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.paths import loading_pipeline_queue_path

logger = get_logger(__name__)


@web.middleware
async def error_middleware(request, handler):
    try:
        return await handler(request)
    except web.HTTPError:
        logger.exception('HTTPError')
        raise
    except Exception as e:
        logger.exception('Unhandled Exception')
        error_reason = f'{e}: {traceback.format_exc()}'
        raise web.HTTPInternalServerError(reason=error_reason) from e


async def loading_pipeline_enqueue(request: web.Request) -> web.Response:
    if not request.body_exists:
        raise web.HTTPUnprocessableEntity

    try:
        lpr = LoadingPipelineRequest.model_validate(await request.json())
    except ValueError as e:
        raise web.HTTPBadRequest from e

    in_progress_request_path = os.path.join(
        loading_pipeline_queue_path(),
        'request.json',
    )
    if await aiofiles.exists(in_progress_request_path):
        async with aiofiles.open(in_progress_request_path, 'r') as f:
            return web.json_response({'In process request': json.loads(await f.read())})

    async with aiofiles.open(in_progress_request_path, 'w') as f:
        await f.write(lpr.model_dump_json())
    return web.json_response({'Successfully queued': lpr.model_dump_json()})


async def status(_: web.Request) -> web.Response:
    return web.json_response({'success': True})


async def init_web_app():
    app = web.Application(middlewares=[error_middleware])
    app.add_routes(
        [
            web.get('/status', status),
            web.post('/loading_pipeline_enqueue', loading_pipeline_enqueue),
        ],
    )
    return app
