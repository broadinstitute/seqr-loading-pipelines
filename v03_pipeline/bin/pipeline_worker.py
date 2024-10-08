#!/usr/bin/env python3
import datetime
import os
import time

import luigi

from v03_pipeline.api.model import LoadingPipelineRequest
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.paths import (
    loading_pipeline_queue_path,
    project_pedigree_path,
    project_remap_path,
)
from v03_pipeline.lib.tasks import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
)
from v03_pipeline.lib.tasks.trigger_hail_backend_reload import TriggerHailBackendReload
from v03_pipeline.lib.tasks.write_success_file import WriteSuccessFileTask

logger = get_logger(__name__)


def main():
    while True:
        try:
            if not os.path.exists(loading_pipeline_queue_path()):
                continue
            with open(loading_pipeline_queue_path()) as f:
                lpr = LoadingPipelineRequest.model_validate_json(f.read())
            project_remap_paths = [
                project_remap_path(
                    lpr.reference_genome,
                    lpr.dataset_type,
                    lpr.sample_type,
                    project_guid,
                )
                for project_guid in lpr.projects_to_run
            ]
            project_pedigree_paths = [
                project_pedigree_path(
                    lpr.reference_genome,
                    lpr.dataset_type,
                    lpr.sample_type,
                    project_guid,
                )
                for project_guid in lpr.projects_to_run
            ]
            run_id = datetime.datetime.now(datetime.timezone.utc).strftime(
                '%Y%m%d-%H%M%S',
            )
            loading_run_task_params = {
                'project_guids': lpr.projects_to_run,
                'project_remap_paths': project_remap_paths,
                'project_pedigree_paths': project_pedigree_paths,
                'run_id': run_id,
                **{k: v for k, v in lpr.model_dump().items() if k != 'projects_to_run'},
            }
            tasks = [
                UpdateVariantAnnotationsTableWithNewSamplesTask(
                    **loading_run_task_params,
                ),
                WriteSuccessFileTask(**loading_run_task_params),
            ]
            if Env.SHOULD_TRIGGER_HAIL_BACKEND_RELOAD:
                tasks.append(
                    TriggerHailBackendReload(**loading_run_task_params),
                )
            luigi.build(tasks)
        except Exception:
            logger.exception('Unhandled Exception')
        finally:
            if os.path.exists(loading_pipeline_queue_path()):
                os.remove(loading_pipeline_queue_path())
            logger.info('Waiting for work')
            time.sleep(1)


if __name__ == '__main__':
    main()
