#!/usr/bin/env python3
import uuid

import luigi

from v03_pipeline.lib.constants import MIGRATION_RUN_ID
from v03_pipeline.lib.model import DatasetType, FeatureFlag, ReferenceGenome
from v03_pipeline.lib.tasks import (
    MigrateAllProjectsToClickHouseOnDataprocTask,
    MigrateAllProjectsToClickHouseTask,
)

if __name__ == '__main__':
    task_cls = (
        MigrateAllProjectsToClickHouseOnDataprocTask
        if FeatureFlag.RUN_PIPELINE_ON_DATAPROC
        else MigrateAllProjectsToClickHouseTask
    )
    luigi.build(
        [
            task_cls(
                reference_genome,
                DatasetType.SNV_INDEL,
                run_id=MIGRATION_RUN_ID + '-' + str(uuid.uuid1().int)[:4],
            )
            for reference_genome in ReferenceGenome
        ],
    )
