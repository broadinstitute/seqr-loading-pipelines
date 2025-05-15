import hail as hl
import hailtop.fs as hfs
import luigi.worker
import pandas as pd

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.paths import (
    clickhouse_migration_flag_file_path,
    new_entries_parquet_path,
    pipeline_run_success_file_path,
    project_table_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.clickhouse_migration.constants import (
    MIGRATION_RUN_ID,
    ClickHouseMigrationType,
)
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_all_project_entries_to_clickhouse import (
    MigrateAllProjectEntriesToClickHouseTask,
)
from v03_pipeline.lib.test.mocked_reference_datasets_testcase import (
    MockedReferenceDatasetsTestCase,
)

TEST_SNV_INDEL_ANNOTATIONS = (
    'v03_pipeline/var/test/exports/GRCh37/SNV_INDEL/annotations.ht'
)
TEST_PROJECT_TABLES = [
    (
        'v03_pipeline/var/test/exports/GRCh37/SNV_INDEL/projects/WGS/R0113_test_project.ht',
        SampleType.WGS,
        'R0113_test_project',
    ),
    (
        'v03_pipeline/var/test/exports/GRCh37/SNV_INDEL/projects/WES/R0113_test_project.ht',
        SampleType.WES,
        'R0113_test_project',
    ),
    (
        'v03_pipeline/var/test/exports/GRCh37/SNV_INDEL/projects/WES/R0114_project4.ht',
        SampleType.WES,
        'R0114_project4',
    ),
]


class MigrateAllProjectEntriesToClickHouseTaskTest(MockedReferenceDatasetsTestCase):
    def setUp(self) -> None:
        super().setUp()
        ht = hl.read_table(TEST_SNV_INDEL_ANNOTATIONS)
        ht.write(
            variant_annotations_table_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
            ),
        )
        for path, sample_type, project_guid in TEST_PROJECT_TABLES:
            ht = hl.read_table(path)
            ht.write(
                project_table_path(
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV_INDEL,
                    sample_type,
                    project_guid,
                ),
            )

    def test_all_project_entries_to_clickhouse_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = MigrateAllProjectEntriesToClickHouseTask(
            reference_genome=ReferenceGenome.GRCh37,
            dataset_type=DatasetType.SNV_INDEL,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        for _, sample_type, project_guid in TEST_PROJECT_TABLES:
            df = pd.read_parquet(
                new_entries_parquet_path(
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV_INDEL,
                    f'{MIGRATION_RUN_ID}_{sample_type.value}_{project_guid}',
                ),
            )
            export_json = df.to_dict('records')
            self.assertEqual(
                export_json[0]['key'],
                1424,
            )

        with hfs.open(
            clickhouse_migration_flag_file_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
                f'{MIGRATION_RUN_ID}_{sample_type.value}_{project_guid}',
                ClickHouseMigrationType.PROJECT_ENTRIES,
            ),
        ) as f:
            self.assertEqual(f.read(), '')

        with hfs.open(
            pipeline_run_success_file_path(
                ReferenceGenome.GRCh37,
                DatasetType.SNV_INDEL,
                f'{MIGRATION_RUN_ID}_{sample_type.value}_{project_guid}',
            ),
        ) as f:
            self.assertEqual(f.read(), '')
