import traceback

import aiofiles
import aiofiles.os
from aiohttp import web, web_exceptions

from v03_pipeline.api.model import LoadingPipelineRequest
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.runs import is_queue_full, new_run_id
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    loading_pipeline_queue_dir,
    loading_pipeline_queue_path,
)

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

    if is_queue_full():
        return web.json_response(
            {
                f'Loading pipeline queue is full. Please try again later. (limit={Env.LOADING_QUEUE_LIMIT})',
            },
            status=web_exceptions.HTTPConflict.status_code,
        )

    try:
        lpr = LoadingPipelineRequest.model_validate(await request.json())
    except ValueError as e:
        raise web.HTTPBadRequest from e

    run_id = new_run_id()
    async with aiofiles.open(loading_pipeline_queue_path(run_id), 'w') as f:
        await f.write(lpr.model_dump_json())
    return web.json_response(
        {'Successfully queued': lpr.model_dump()},
        status=web_exceptions.HTTPAccepted.status_code,
    )


async def status(_: web.Request) -> web.Response:
    return web.json_response({'success': True})


async def init_web_app():
    await aiofiles.os.makedirs(
        loading_pipeline_queue_dir(),
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
