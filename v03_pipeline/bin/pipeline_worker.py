#!/usr/bin/env python3
import os
import time

import luigi

from v03_pipeline.api.model import LoadingPipelineRequest
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.paths import (
    loading_pipeline_queue_path,
    project_pedigree_path,
    project_remap_path,
)
from v03_pipeline.lib.tasks import (
    UpdateVariantAnnotationsTableWithNewSamplesTask,
    WriteProjectFamilyTablesTask,
)

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
            luigi.build(
                [
                    WriteProjectFamilyTablesTask(
                        project_guid=lpr.projects_to_run[i],
                        project_remap_path=project_remap_paths[i],
                        project_pedigree_path=project_pedigree_paths[i],
                        force=False,
                        **{
                            k: v
                            for k, v in lpr.model_dump().items()
                            if k != 'projects_to_run'
                        },
                    )
                    for i in range(len(lpr.projects_to_run))
                ],
                UpdateVariantAnnotationsTableWithNewSamplesTask(
                    project_guids=lpr.projects_to_run,
                    project_remap_paths=project_remap_paths,
                    project_pedigree_paths=project_pedigree_paths,
                    force=False,
                    **{
                        k: v
                        for k, v in lpr.model_dump().items()
                        if k != 'projects_to_run'
                    },
                ),
            )
        except Exception:
            logger.exception('Unhandled Exception')
        finally:
            logger.info('Waiting for work')
            time.sleep(1)


if __name__ == '__main__':
    main()
