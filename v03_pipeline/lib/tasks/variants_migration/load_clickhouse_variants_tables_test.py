from unittest.mock import Mock, patch

import hail as hl
import luigi.worker

from v03_pipeline.lib.core import (
    DatasetType,
    ReferenceGenome,
)
from v03_pipeline.lib.core.environment import Env
from v03_pipeline.lib.misc.clickhouse import (
    STAGING_CLICKHOUSE_DATABASE,
    ClickhouseReferenceDataset,
    get_clickhouse_client,
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

TEST_SCHEMA = 'v03_pipeline/var/test/test_clickhouse_schema.sql'
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
        client = get_clickhouse_client()
        client.execute(
            f"""
            DROP DATABASE IF EXISTS {STAGING_CLICKHOUSE_DATABASE};
            """,
        )
        client.execute(
            f"""
            DROP DATABASE IF EXISTS {Env.CLICKHOUSE_DATABASE};
            """,
        )
        client.execute(
            f"""
            CREATE DATABASE {Env.CLICKHOUSE_DATABASE};
        """,
        )
        client = get_clickhouse_client(database=Env.CLICKHOUSE_DATABASE)
        client.execute(
            """
            CREATE DICTIONARY seqrdb_gene_ids
            (
                `gene_id` String,
                `seqrdb_id` String,
                `affected` String
            )
            PRIMARY KEY gene_id
            SOURCE(NULL())
            LIFETIME(0)
            LAYOUT(HASHED())
            """,
        )
        client.execute(
            """
            CREATE DICTIONARY seqrdb_affected_status_dict
            (
                `family_guid` String,
                `sampleId` String,
                `affected` String
            )
            PRIMARY KEY family_guid, sampleId
            SOURCE(NULL())
            LIFETIME(0)
            LAYOUT(COMPLEX_KEY_HASHED())
            """,
        )
        client.execute(
            """
            CREATE DICTIONARY `GRCh38/SNV_INDEL/project_partitions_dict`
            (
                `project_guid` String,
                `n_partitions` UInt32
            )
            PRIMARY KEY project_guid
            SOURCE(NULL())
            LIFETIME(0)
            LAYOUT(COMPLEX_KEY_HASHED())
            """,
        )
        client.execute(
            """
            CREATE DICTIONARY `GRCh38/SNV_INDEL/reference_data/gnomad_genomes`
            (
                `key` UInt32,
                `filter_af` Decimal(9, 8)
            )
            PRIMARY KEY key
            SOURCE(NULL())
            LIFETIME(0)
            LAYOUT(COMPLEX_KEY_HASHED())
            """,
        )
        with open(TEST_SCHEMA) as f:
            sql = f.read()
        commands = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]
        for cmd in commands:
            client.execute(cmd)
        client.execute(
            f"""
            CREATE DICTIONARY `GRCh38/SNV_INDEL/gt_stats_dict`
            (
                `key` UInt32,
                `ac_wes` UInt64,
                `ac_wgs` UInt64,
                `ac_affected` UInt64,
                `hom_wes` UInt64,
                `hom_wgs` UInt64,
                `hom_affected` UInt64
            )
            PRIMARY KEY key
            SOURCE(
                CLICKHOUSE(
                    USER {Env.CLICKHOUSE_WRITER_USER} PASSWORD {Env.CLICKHOUSE_WRITER_PASSWORD}
                    DB {Env.CLICKHOUSE_DATABASE} TABLE `GRCh38/SNV_INDEL/gt_stats`
                )
            )
            LIFETIME(0)
            LAYOUT(FLAT(MAX_ARRAY_SIZE 1000000000))
            """,
        )

    def tearDown(self):
        super().tearDown()
        client = get_clickhouse_client()
        client.execute(
            f"""
           DROP DATABASE IF EXISTS {STAGING_CLICKHOUSE_DATABASE};
           """,
        )
        client.execute(
            f"""
           DROP DATABASE IF EXISTS {Env.CLICKHOUSE_DATABASE};
           """,
        )

    @patch.object(
        ClickhouseReferenceDataset,
        'for_reference_genome_dataset_type',
        return_value=[ClickhouseReferenceDataset.CLINVAR],
    )
    def test_write_variants_to_clickhouse(
        self,
        mock_for_reference_genome_dataset_type: Mock,
    ):
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
