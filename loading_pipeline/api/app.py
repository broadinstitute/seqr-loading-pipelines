import traceback

import aiofiles
import aiofiles.os
from aiohttp import web, web_exceptions

from loading_pipeline.api.model import (
    DeleteFamiliesRequest,
    LoadingPipelineRequest,
    PipelineRunnerRequest,
    RebuildGtStatsRequest,
    RefreshClickhouseReferenceDataRequest,
)
from loading_pipeline.lib.core.environment import Env
from loading_pipeline.lib.logger import get_logger
from loading_pipeline.lib.misc.runs import is_queue_full, new_run_id
from loading_pipeline.lib.paths import (
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


async def _enqueue_request(
    request: web.Request,
    model_cls: type[PipelineRunnerRequest],
) -> web.Response:
    """Generic helper to enqueue a pipeline request of any type."""
    if not request.body_exists:
        raise web.HTTPUnprocessableEntity

    if is_queue_full():
        return web.json_response(
            f'Pipeline queue is full. Please try again later. (limit={Env.LOADING_QUEUE_LIMIT})',
            status=web_exceptions.HTTPConflict.status_code,
        )

    try:
        model_instance = model_cls.model_validate(await request.json())
    except ValueError as e:
        raise web.HTTPBadRequest from e

    queue_path = loading_pipeline_queue_path(new_run_id())
    async with aiofiles.open(queue_path, 'w') as f:
        await f.write(model_instance.model_dump_json())

    return web.json_response(
        {'Successfully queued': model_instance.model_dump()},
        status=web_exceptions.HTTPAccepted.status_code,
    )


TASK_TYPE_TO_MODEL = {
    'loading_pipeline': LoadingPipelineRequest,
    'delete_families': DeleteFamiliesRequest,
    'rebuild_gt_stats': RebuildGtStatsRequest,
    'refresh_clickhouse_reference_dataset': RefreshClickhouseReferenceDataRequest,
}


async def tasks_queue(request: web.Request) -> web.Response:
    task_type = request.match_info['task_type']
    if task_type not in TASK_TYPE_TO_MODEL:
        raise web.HTTPNotFound()
    return await _enqueue_request(request, TASK_TYPE_TO_MODEL[task_type])


async def loading_pipeline_enqueue(request: web.Request) -> web.Response:
    return web.Response(
        status=web_exceptions.HTTPMovedPermanently.status_code,
        headers={'Location': '/tasks/loading_pipeline/queue'},
    )


async def delete_families_enqueue(request: web.Request) -> web.Response:
    return web.Response(
        status=web_exceptions.HTTPMovedPermanently.status_code,
        headers={'Location': '/tasks/delete_families/queue'},
    )


async def rebuild_gt_stats_enqueue(request: web.Request) -> web.Response:
    return web.Response(
        status=web_exceptions.HTTPMovedPermanently.status_code,
        headers={'Location': '/tasks/rebuild_gt_stats/queue'},
    )


async def refresh_clickhouse_reference_dataset_enqueue(
    request: web.Request,
) -> web.Response:
    return web.Response(
        status=web_exceptions.HTTPMovedPermanently.status_code,
        headers={'Location': '/tasks/refresh_clickhouse_reference_dataset/queue'},
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
            web.post('/tasks/{task_type}/queue', tasks_queue),
            # Legacy routes (redirect to new ones)
            web.post('/loading_pipeline_enqueue', loading_pipeline_enqueue),
            web.post('/delete_families_enqueue', delete_families_enqueue),
            web.post('/rebuild_gt_stats_enqueue', rebuild_gt_stats_enqueue),
            web.post(
                '/refresh_clickhouse_reference_dataset_enqueue',
                refresh_clickhouse_reference_dataset_enqueue,
            ),
        ],
    )
    return app
