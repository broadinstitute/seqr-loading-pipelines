#!/usr/bin/env python3
import json
import os
import re
import signal
import sys
import time

from v03_pipeline.api.model import (
    PipelineRunnerRequest,
)
from v03_pipeline.api.request_handlers import REQUEST_HANDLER_MAP
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.clickhouse import (
    drop_staging_db,
)
from v03_pipeline.lib.misc.runs import get_oldest_queue_path
from v03_pipeline.lib.misc.slack import (
    safe_post_to_slack_failure,
    safe_post_to_slack_success,
)

logger = get_logger(__name__)


def signal_handler(*_):
    drop_staging_db()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def parse_latest_queue_path(
    latest_queue_path: str,
) -> tuple[PipelineRunnerRequest, str]:
    run_id = re.search(
        r'request_(\d{8}-\d{6}-\d{6})\.json',
        os.path.basename(latest_queue_path),
    ).group(1)
    with open(latest_queue_path) as f:
        raw_json = json.load(f)
    request_type_name = raw_json['request_type']
    request_cls = next(
        (cls for cls in REQUEST_HANDLER_MAP if cls.__name__ == request_type_name),
        None,
    )
    if not request_cls:
        msg = f'Unknown request_type: {request_type_name}'
        raise ValueError(msg)
    prr = request_cls.model_validate(raw_json)
    return prr, run_id


def process_queue(local_scheduler=False):
    run_id = None
    try:
        latest_queue_path = get_oldest_queue_path()
        if latest_queue_path is None:
            return
        prr, run_id = parse_latest_queue_path(latest_queue_path)
        REQUEST_HANDLER_MAP[type(prr)](prr, run_id, local_scheduler)
        safe_post_to_slack_success(
            run_id,
            prr,
        )
    except Exception as e:
        logger.exception('Unhandled Exception')
        if run_id is not None:
            safe_post_to_slack_failure(
                run_id,
                prr,
                e,
            )
    finally:
        if latest_queue_path is not None and os.path.exists(latest_queue_path):
            os.remove(latest_queue_path)
        logger.info('Looking for more work')
        time.sleep(1)


def main():
    while True:
        process_queue()


if __name__ == '__main__':
    main()
