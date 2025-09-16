#!/usr/bin/env python3
import os
import re
import time

import luigi

from v03_pipeline.api.model import LoadingPipelineRequest
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.runs import get_oldest_queue_path
from v03_pipeline.lib.misc.slack import (
    safe_post_to_slack_failure,
    safe_post_to_slack_success,
)
from v03_pipeline.lib.tasks.write_success_file import WriteSuccessFileTask

logger = get_logger(__name__)

def process_queue(local_scheduler=False):
    try:
        latest_queue_path = get_oldest_queue_path()
        if latest_queue_path is None:
            continue
        with open(latest_queue_path) as f:
            lpr = LoadingPipelineRequest.model_validate_json(f.read())
        run_id = re.search(
            r'request_(\d{8}-\d{6})_\d+\.json',
            os.path.basename(latest_queue_path),
        ).group(1)
        loading_run_task_params = {
            'project_guids': lpr.projects_to_run,
            'run_id': run_id,
            **{k: v for k, v in lpr.model_dump().items() if k != 'projects_to_run'},
        }
        tasks = [
            WriteSuccessFileTask(**loading_run_task_params),
        ]
        luigi.build(tasks, local_scheduler=local_scheduler)
        safe_post_to_slack_success(
            run_id,
            lpr,
        )
    except Exception as e:
        logger.exception('Unhandled Exception')
        safe_post_to_slack_failure(
            run_id,
            lpr,
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
