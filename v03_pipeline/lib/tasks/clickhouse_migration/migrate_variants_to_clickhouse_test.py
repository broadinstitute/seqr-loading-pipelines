import json

import hail as hl
import hailtop.fs as hfs
import luigi.worker
import pandas as pd

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    metadata_for_run_path,
    new_clinvar_variants_parquet_path,
    new_transcripts_parquet_path,
    new_variants_parquet_path,
    pipeline_run_success_file_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.clickhouse_migration.constants import (
    ClickHouseMigrationType,
)
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_variants_to_clickhouse import (
    MigrateVariantsToClickHouseTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import (
    MockedDatarootTestCase,
)

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh38/SNV_INDEL/annotations.ht'
)


class WriteVariantsToClickHouseTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        ht = hl.read_table(TEST_SNV_INDEL_ANNOTATIONS)
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        )

    def test_migrate_variants_to_clickhouse_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = MigrateVariantsToClickHouseTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_clinvar_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ClickHouseMigrationType.VARIANTS.run_id,
            ),
        )
        self.assertEqual(df.shape[0], 2)
        df = pd.read_parquet(
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ClickHouseMigrationType.VARIANTS.run_id,
            ),
        )
        self.assertEqual(df.shape[0], 2)
        df = pd.read_parquet(
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ClickHouseMigrationType.VARIANTS.run_id,
            ),
        )
        self.assertEqual(df.shape[0], 2)
        with hfs.open(
            metadata_for_run_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ClickHouseMigrationType.VARIANTS.run_id,
            ),
        ) as f:
            metadata_json = json.load(f)
            self.assertEqual(
                metadata_json['run_id'],
                ClickHouseMigrationType.VARIANTS.run_id,
            )
            self.assertEqual(
                metadata_json['migration_type'],
                ClickHouseMigrationType.VARIANTS.value,
            )

        with hfs.open(
            pipeline_run_success_file_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ClickHouseMigrationType.VARIANTS.run_id,
            ),
        ) as f:
            self.assertEqual(f.read(), '')
