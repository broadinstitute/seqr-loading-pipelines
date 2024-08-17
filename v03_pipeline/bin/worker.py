#!/usr/bin/env python3
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
    WriteProjectFamilyTablesTask,
)

logger = get_logger(__name__)


def main():
    while True:
        try:
            if not os.path.exists(loading_pipeline_queue_path()):
                continue
            with open(loading_pipeline_queue_path()) as f:
                lpr = LoadingPipelineRequest.model_validate_json(json.load(f))
                luigi.build(
                    [
                        WriteProjectFamilyTablesTask(
                            callset_path=lpr.callset_path,
                            project_guid=project_guid,
                            project_remap_path=project_remap_path(
                                lpr.reference_genome,
                                lpr.dataset_type,
                                lpr.sample_type,
                                lpr.project_guid,
                            ),
                            project_pedigree_path=project_pedigree_path(
                                lpr.reference_genome,
                                lpr.dataset_type,
                                lpr.sample_type,
                                lpr.project_guid,
                            ),
                            reference_genome=lpr.reference_genome,
                            dataset_type=lpr.dataset_type,
                            sample_type=lpr.sample_type,
                            force=lpr.force,
                            ignore_missing_samples_when_remapping=lpr.ignore_missing_samples_when_remapping,
                            skip_validation=lpr.skip_validation,
                        )
                        for project_guid in lpt.projects_to_run
                    ],
                )
        except Exception:
            logger.exception('Unhandled Exception')
        finally:
            logger.info('Waiting for work')
            time.sleep(3)


if __name__ == '__main__':
    main()
