#!/usr/bin/env python3
import re
import os
import time

import luigi

from v03_pipeline.api.model import LoadingPipelineRequest
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import FeatureFlag
from v03_pipeline.lib.paths import (
    get_oldest_queue_path,
    project_pedigree_path,
)
from v03_pipeline.lib.tasks.trigger_hail_backend_reload import TriggerHailBackendReload
from v03_pipeline.lib.tasks.write_success_file import WriteSuccessFileTask

logger = get_logger(__name__)


def main():
    while True:
        try:
            latest_queue_path = get_oldest_queue_path()
            if latest_queue_path is None:
                continue
            with open(latest_queue_path) as f:
                lpr = LoadingPipelineRequest.model_validate_json(f.read())
            project_pedigree_paths = [
                project_pedigree_path(
                    lpr.reference_genome,
                    lpr.dataset_type,
                    lpr.sample_type,
                    project_guid,
                )
                for project_guid in lpr.projects_to_run
            ]
            run_id = re.search(
                r'request_(\d{8}-\d{6})_\d+\.json',
                os.path.basename(latest_queue_path),
            ).group(1)
            loading_run_task_params = {
                'project_guids': lpr.projects_to_run,
                'project_pedigree_paths': project_pedigree_paths,
                'run_id': run_id,
                **{k: v for k, v in lpr.model_dump().items() if k != 'projects_to_run'},
            }
            if FeatureFlag.SHOULD_TRIGGER_HAIL_BACKEND_RELOAD:
                tasks = [
                    TriggerHailBackendReload(**loading_run_task_params),
                ]
            else:
                tasks = [
                    WriteSuccessFileTask(**loading_run_task_params),
                ]
            luigi.build(tasks)
        except Exception:
            logger.exception('Unhandled Exception')
        finally:
            if latest_queue_path is not None and os.path.exists(latest_queue_path):
                os.remove(latest_queue_path)
            logger.info('Looking for more work')
            time.sleep(1)


if __name__ == '__main__':
    main()
