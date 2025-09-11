#!/usr/bin/env python3

import luigi

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks import MigrateAllProjectsToClickHouseTask

if __name__ == '__main__':
    luigi.build(
        [
            MigrateAllProjectsToClickHouseTask(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
            ),
            MigrateAllProjectsToClickHouseTask(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        ],
    )
