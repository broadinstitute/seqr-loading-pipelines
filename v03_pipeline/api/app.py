import json
import os
import traceback

import aiofiles
import aiofiles.os
from aiohttp import web, web_exceptions

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

    try:
        async with aiofiles.open(loading_pipeline_queue_path(), 'r') as f:
            return web.json_response(
                {
                    'Failed to queue due to in process request': json.loads(
                        await f.read(),
                    ),
                },
                #
                # The 409 (Conflict) status code indicates that the request
                # could not be completed due to a conflict with the current
                # state of the target resource.
                #
                status=web_exceptions.HTTPConflict.status_code,
            )
    except FileNotFoundError:
        pass

    async with aiofiles.open(loading_pipeline_queue_path(), 'w') as f:
        await f.write(lpr.model_dump_json())
    return web.json_response(
        {'Successfully queued': lpr.model_dump()},
        status=web_exceptions.HTTPAccepted.status_code,
    )


async def status(_: web.Request) -> web.Response:
    return web.json_response({'success': True})


async def init_web_app():
    await aiofiles.os.makedirs(
        os.path.dirname(loading_pipeline_queue_path()),
        exist_ok=True,
    )
    app = web.Application(middlewares=[error_middleware])
    app.add_routes(
        [
            web.get('/status', status),
            web.post('/loading_pipeline_enqueue', loading_pipeline_enqueue),
        ],
    )
    return app
