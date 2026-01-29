import hail as hl
import luigi.worker

from v03_pipeline.lib.core import (
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import (
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.variants_migration.migrate_variant_details_parquet import (
    MigrateVariantDetailsParquetTask,
)
from v03_pipeline.lib.tasks.variants_migration.migrate_variants_parquet import (
    MigrateVariantsParquetTask,
)
from v03_pipeline.lib.tasks.variants_migration.update_variant_annotations_table_with_dropped_reference_datasets import (
    UpdateVariantAnnotationsTableWithDroppedReferenceDatasetsTask,
)
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedDatarootTestCase,
)

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh38/SNV_INDEL/annotations.ht'
)
TEST_RUN_ID = 'manual__2024-04-03'


class LoadClickhouseVariantsTablesTaskTest(MockedDatarootTestCase):
    def setUp(self) -> None:
        super().setUp()
        ht = hl.read_table(TEST_SNV_INDEL_ANNOTATIONS)
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
            ),
        )

    def test_write_variants_parquets(self):
        worker = luigi.worker.Worker()
        task = UpdateVariantAnnotationsTableWithDroppedReferenceDatasetsTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
        ht = hl.read_table(task.output().path)
        self.assertTrue('variant_id' in ht.row)
        self.assertTrue('gnomad_genomes' not in ht.row and 'dbnsfp' not in ht.row)

        task = MigrateVariantsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            attempt_id=0,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())

        task = MigrateVariantDetailsParquetTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            attempt_id=0,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())
