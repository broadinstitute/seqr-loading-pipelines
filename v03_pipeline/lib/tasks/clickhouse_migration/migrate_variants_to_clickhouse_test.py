import hail as hl
import luigi.worker
import pandas as pd

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    new_clinvar_variants_parquet_path,
    new_transcripts_parquet_path,
    new_variants_parquet_path,
    pipeline_run_success_file_path,
    variant_annotations_table_path,
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

TEST_RUN_ID = 'manual__migration_2024-04-03'


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
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        df = pd.read_parquet(
            new_clinvar_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        self.assertEqual(df.shape[0], 2)
        df = pd.read_parquet(
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        self.assertEqual(df.shape[0], 2)
        df = pd.read_parquet(
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        self.assertEqual(df.shape[0], 2)
        with open(
            pipeline_run_success_file_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        ) as f:
            self.assertEqual(f.read(), '_VARIANTS_MIGRATION')
