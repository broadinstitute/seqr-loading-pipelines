import json
import os
import shutil

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
    db_id_to_gene_id_path,
    metadata_for_run_path,
    new_entries_parquet_path,
    pipeline_run_success_file_path,
    project_table_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.clickhouse_migration.migrate_all_projects_to_clickhouse import (
    MIGRATION_RUN_ID,
    MigrateAllProjectsToClickHouseTask,
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
TEST_DB_ID_TO_GENE_ID = 'v03_pipeline/var/test/db_id_to_gene_id.csv.gz'


class MigrateAllProjectsToClickHouseTaskTest(MockedReferenceDatasetsTestCase):
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
        os.makedirs(
            self.mock_env.LOADING_DATASETS_DIR,
            exist_ok=True,
        )
        shutil.copy2(
            TEST_DB_ID_TO_GENE_ID,
            db_id_to_gene_id_path(),
        )

    def test_all_project_entries_to_clickhouse_test(
        self,
    ) -> None:
        worker = luigi.worker.Worker()
        task = MigrateAllProjectsToClickHouseTask(
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

        for _, sample_type, project_guid in TEST_PROJECT_TABLES:
            with hfs.open(
                metadata_for_run_path(
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV_INDEL,
                    f'{MIGRATION_RUN_ID}_{sample_type.value}_{project_guid}',
                ),
            ) as f:
                metadata_json = json.load(f)
                self.assertEqual(
                    metadata_json['run_id'],
                    f'{MIGRATION_RUN_ID}_{sample_type.value}_{project_guid}',
                )
                self.assertEqual(
                    metadata_json['family_samples']['F079280_bh10261'],
                    ['BH10261-1', 'BH10261-2', 'BH10261-3-T'],
                )

            with hfs.open(
                pipeline_run_success_file_path(
                    ReferenceGenome.GRCh37,
                    DatasetType.SNV_INDEL,
                    f'{MIGRATION_RUN_ID}_{sample_type.value}_{project_guid}',
                ),
            ) as f:
                self.assertEqual(f.read(), '')
