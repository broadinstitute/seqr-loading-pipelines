import os
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from v03_pipeline.lib.misc.clickhouse import (
    STAGING_CLICKHOUSE_DATABASE,
    ClickHouseDictionary,
    ClickHouseMaterializedView,
    ClickHouseTable,
    TableNameBuilder,
    create_staging_non_table_entities,
    create_staging_tables,
    delete_existing_families_from_staging_entries,
    direct_insert_all_keys,
    exchange_tables,
    get_clickhouse_client,
    insert_new_entries,
    load_complete_run,
    optimize_entries,
    refresh_materialized_views,
    reload_dictionaries,
    replace_project_partitions,
    stage_existing_project_partitions,
)
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
    new_transcripts_parquet_path,
    new_variants_parquet_path,
    runs_path,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_RUN_ID = 'manual__2025-05-07T17-20-59.702114+00-00'


class ClickhouseTest(MockedDatarootTestCase):
    def setUp(self):
        super().setUp()
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
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries` (
                `key` UInt32,
                `project_guid` LowCardinality(String),
                `family_guid` String,
                `xpos` UInt64 CODEC(Delta(8), ZSTD(1)),
                `sample_type` Enum8('WES' = 0, 'WGS' = 1),
                `is_annotated_in_any_gene` Boolean,
                `geneId_ids` AggregateFunction(groupBitmap, UInt32),
                `calls` Array(
                    Tuple(
                        sampleId String,
                        gt Nullable(Enum8('REF' = 0, 'HET' = 1, 'HOM' = 2)),
                    )
                ),
                `sign` Int8,
                PROJECTION xpos_projection
                (
                    SELECT *
                    ORDER BY is_annotated_in_any_gene, xpos
                )
            )
            ENGINE = CollapsingMergeTree(sign)
            PARTITION BY project_guid
            ORDER BY (project_guid, family_guid, key)
            SETTINGS deduplicate_merge_projection_mode = 'rebuild';
            """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/project_gt_stats`
            (
                `project_guid` String,
                `key` UInt32,
                `sample_type` Enum8('WES' = 0, 'WGS' = 1),
                `het_samples` Int32,
                `hom_samples` Int32,
            )
            ENGINE = SummingMergeTree
            PARTITION BY project_guid
            ORDER BY (project_guid, key, sample_type)
            """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats`
            (
                `key` UInt32,
                `ac_wes` UInt32,
                `ac_wgs` UInt32,
            )
            ENGINE = SummingMergeTree
            ORDER BY (key)
            """,
        )
        client.execute(
            f"""
            CREATE MATERIALIZED VIEW {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries_to_project_gt_stats_mv`
            TO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/project_gt_stats`
            AS SELECT
                project_guid,
                key,
                sample_type,
                sum(toInt32(arrayCount(s -> (s.gt = 'HET'), calls) * sign)) AS het_samples,
                sum(toInt32(arrayCount(s -> (s.gt = 'HOM'), calls) * sign)) AS hom_samples
            FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries`
            GROUP BY project_guid, key, sample_type
            """,
        )
        client.execute(
            f"""
            CREATE MATERIALIZED VIEW {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/project_gt_stats_to_gt_stats_mv`
            REFRESH EVERY 10 YEAR TO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats`
            AS SELECT
                key,
                sumIf(het_samples * 1 + hom_samples * 2, sample_type = 'WES') AS ac_wes,
                sumIf(het_samples * 1 + hom_samples * 2, sample_type = 'WGS') AS ac_wgs
            FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/project_gt_stats`
            GROUP BY key
            """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts` (
                key UInt32,
                transcripts String
            ) ENGINE = EmbeddedRocksDB()
            PRIMARY KEY `key`
        """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/annotations_memory` (
                key UInt32,
                variantId String,
            ) ENGINE = EmbeddedRocksDB()
            PRIMARY KEY `key`
        """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/annotations_disk` (
                key UInt32,
                variantId String,
            ) ENGINE = EmbeddedRocksDB()
            PRIMARY KEY `key`
        """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/key_lookup` (
                variantId String,
                key UInt32,
            ) ENGINE = EmbeddedRocksDB()
            PRIMARY KEY `variantId`
        """,
        )
        client.execute(
            f"""
            CREATE DICTIONARY {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats_dict`
            (
                `key` UInt32,
                `ac_wes` UInt16,
                `ac_wgs` UInt16
            )
            PRIMARY KEY key
            SOURCE(
                CLICKHOUSE(
                    USER {Env.CLICKHOUSE_WRITER_USER} PASSWORD {Env.CLICKHOUSE_WRITER_PASSWORD}
                    DB {Env.CLICKHOUSE_DATABASE} TABLE `GRCh38/SNV_INDEL/gt_stats`
                )
            )
            LIFETIME(0)
            LAYOUT(FLAT(MAX_ARRAY_SIZE 10000))
            """,
        )
        client.execute(
            f"""
            CREATE MATERIALIZED VIEW {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/clinvar_all_variants_to_clinvar_mv`
            REFRESH EVERY 10 YEAR ENGINE = Null
            AS SELECT *
            FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/key_lookup`
            """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/GCNV/entries` (
                `key` UInt32,
                `project_guid` LowCardinality(String),
                `family_guid` String,
                `xpos` UInt64 CODEC(Delta(8), ZSTD(1)),
                `sample_type` Enum8('WES' = 0, 'WGS' = 1),
                `calls` Array(
                    Tuple(
                        sampleId String,
                        gt Nullable(Enum8('REF' = 0, 'HET' = 1, 'HOM' = 2)),
                    )
                ),
                `sign` Int8,
                PROJECTION xpos_projection
                (
                    SELECT *
                    ORDER BY xpos
                )
            )
            ENGINE = CollapsingMergeTree(sign)
            PARTITION BY project_guid
            ORDER BY (project_guid, family_guid, key)
            SETTINGS deduplicate_merge_projection_mode = 'rebuild';
            """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/GCNV/annotations_memory` (
                key UInt32,
                variantId String,
            ) ENGINE = EmbeddedRocksDB()
            PRIMARY KEY `key`
        """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/GCNV/annotations_disk` (
                key UInt32,
                variantId String,
            ) ENGINE = EmbeddedRocksDB()
            PRIMARY KEY `key`
        """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/GCNV/key_lookup` (
                variantId String,
                key UInt32,
            ) ENGINE = EmbeddedRocksDB()
            PRIMARY KEY `variantId`
        """,
        )
        base_path = runs_path(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
        )
        os.makedirs(os.path.join(base_path, TEST_RUN_ID), exist_ok=True)

        def write_test_parquet(df: pd.DataFrame, parquet_path: str, schema=None):
            if schema:
                table = pa.Table.from_pandas(df, schema=schema)
            else:
                table = pa.Table.from_pandas(df)
            os.makedirs(parquet_path)
            pq.write_table(
                table,
                os.path.join(
                    parquet_path,
                    'test.parquet',
                ),
            )

        # Transcripts Parquet
        df = pd.DataFrame({'key': [1, 2, 3, 4], 'transcripts': ['a', 'b', 'c', 'd']})
        write_test_parquet(
            df,
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )

        # New Variants parquet.
        df = pd.DataFrame(
            {
                'key': [10, 11, 12, 13],
                'variantId': [
                    '1-3-A-C',
                    '2-4-A-T',
                    'Y-9-A-C',
                    'M-2-C-G',
                ],
                'populations': [
                    {'gnomad_genomes': {'filter_af': None}},
                    {'gnomad_genomes': {'filter_af': 0.1}},
                    {'gnomad_genomes': {'filter_af': 0.01}},
                    {'gnomad_genomes': {'filter_af': 0.001}},
                ],
            },
        )
        write_test_parquet(
            df,
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        write_test_parquet(
            df,
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                TEST_RUN_ID,
            ),
        )

        # New Entries Parquet
        df = pd.DataFrame(
            {
                'key': [0, 3, 4],
                'project_guid': [
                    'project_d',
                    'project_d',
                    'project_d',
                ],
                'family_guid': [
                    'family_d1',
                    'family_d2',
                    'family_d3',
                ],
                'xpos': [
                    123456789,
                    123456789,
                    123456789,
                ],
                'sample_type': [
                    'WES',
                    'WES',
                    'WES',
                ],
                'is_annotated_in_any_gene': [
                    True,
                    False,
                    True,
                ],
                'geneId_ids': [
                    [],
                    [123, 12],
                    [1],
                ],
                'calls': [
                    [('sample_d1', 0), ('sample_d11', 2)],
                    [('sample_d2', 0)],
                    [('sample_d3', 1)],
                ],
                'sign': [
                    1,
                    1,
                    1,
                ],
            },
        )
        schema = pa.schema(
            [
                ('key', pa.int64()),
                ('project_guid', pa.string()),
                ('family_guid', pa.string()),
                ('xpos', pa.int64()),
                ('sample_type', pa.string()),
                ('is_annotated_in_any_gene', pa.bool_()),
                ('geneId_ids', pa.list_(pa.int64())),
                (
                    'calls',
                    pa.list_(
                        pa.struct([('sampleId', pa.string()), ('gt', pa.int64())]),
                    ),
                ),
                ('sign', pa.int64()),
            ],
        )
        write_test_parquet(
            df,
            new_entries_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
            schema,
        )
        write_test_parquet(
            df.drop('is_annotated_in_any_gene', axis=1).drop('geneId_ids', axis=1),
            new_entries_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.GCNV,
                TEST_RUN_ID,
            ),
            schema.remove(5).remove(5),
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

    def test_get_clickhouse_client(self):
        client = get_clickhouse_client()
        result = client.execute('SELECT 1')
        self.assertEqual(result[0][0], 1)

    def test_table_name_builder(self):
        table_name_builder = TableNameBuilder(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
        )
        self.assertEqual(
            table_name_builder.dst_table(
                ClickHouseTable.ENTRIES,
            ),
            f'{Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries`',
        )
        self.assertEqual(
            table_name_builder.src_table(
                ClickHouseTable.ENTRIES,
            ),
            f"file('{runs_path(ReferenceGenome.GRCh38, DatasetType.SNV_INDEL)}/manual__2025-05-07T17-20-59.702114+00-00/new_entries.parquet/*.parquet', 'Parquet')",
        )
        with patch('v03_pipeline.lib.paths.Env') as mock_env:
            mock_env.HAIL_SEARCH_DATA_DIR = 'gs://mock_bucket'
            self.assertEqual(
                table_name_builder.src_table(
                    ClickHouseTable.ENTRIES,
                ),
                "gcs(clickhouse_search_named_collection, url='https://storage.googleapis.com/mock_bucket/v3.1/GRCh38/SNV_INDEL/runs/manual__2025-05-07T17-20-59.702114+00-00/new_entries.parquet/*.parquet')",
            )

    def test_direct_insert_all_keys(self):
        client = get_clickhouse_client()
        client.execute(
            f'INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts` VALUES',
            [(1, 'a'), (10, 'b'), (7, 'c')],
        )
        direct_insert_all_keys(
            ClickHouseTable.TRANSCRIPTS,
            TableNameBuilder(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        ret = client.execute(
            f'SELECT * FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts`',
        )
        self.assertEqual(
            ret,
            [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (7, 'c'), (10, 'b')],
        )

        # ensure multiple calls are idempotent
        direct_insert_all_keys(
            ClickHouseTable.TRANSCRIPTS,
            TableNameBuilder(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        ret = client.execute(
            f'SELECT * FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts`',
        )
        self.assertEqual(
            ret,
            [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (7, 'c'), (10, 'b')],
        )

    def test_entries_insert_flow(self):
        # Tests individual components of the atomic_insert_entries
        # to validate the state after each step.
        client = get_clickhouse_client()
        client.execute(
            f"""
            INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries`
            VALUES
            (0, 'project_a', 'family_a1', 123456789, 'WES', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_a1','HOM')], 1),
            (1, 'project_a', 'family_a2', 123456789, 'WGS', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_a2','HET')], 1),
            (2, 'project_a', 'family_a3', 133456789, 'WGS', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_a3','HOM')], 1),
            (3, 'project_a', 'family_a4', 133456789, 'WES', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_a4','REF')], 1),
            (4, 'project_a', 'family_a5', 133456789, 'WES', 1, bitmapBuild(CAST([0] AS Array(UInt32))), [('sample_a5','REF'),('sample_a6','HET'),('sample_a7','REF')], 1),
            (4, 'project_a', 'family_a6', 133456789, 'WGS', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_a8','HOM')], 1),
            (0, 'project_b', 'family_b1', 123456789, 'WES', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_b4','REF')], 1),
            (1, 'project_b', 'family_b2', 123456789, 'WES', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_b5','HET')], 1),
            (2, 'project_b', 'family_b2', 123456789, 'WES', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_b5','REF')], 1),
            (3, 'project_b', 'family_b3', 133456789, 'WES', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_b6','HOM')], 1),
            (4, 'project_b', 'family_b3', 133456789, 'WES', 0, bitmapBuild(CAST([] AS Array(UInt32))), [('sample_b6','HOM')], 1),
            (0, 'project_c', 'family_c1', 123456789, 'WES', 1, bitmapBuild(CAST([1] AS Array(UInt32))), [('sample_c7','REF')], 1),
            (3, 'project_c', 'family_c2', 123456789, 'WES', 1, bitmapBuild(CAST([1] AS Array(UInt32))), [('sample_c8','REF')], 1),
            (4, 'project_c', 'family_c3', 133456789, 'WES', 1, bitmapBuild(CAST([1] AS Array(UInt32))), [('sample_c9','HOM')], 1),
            (5, 'project_c', 'family_c4', 133456789, 'WES', 1, bitmapBuild(CAST([1] AS Array(UInt32))), [('sample_c9','HOM')], 1)
            """,
        )
        table_name_builder = TableNameBuilder(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
        )
        create_staging_tables(
            table_name_builder,
            ClickHouseTable.for_dataset_type_atomic_insert_entries(
                DatasetType.SNV_INDEL,
            ),
        )
        create_staging_non_table_entities(
            table_name_builder,
            [
                *ClickHouseMaterializedView.for_dataset_type_atomic_insert_entries(
                    DatasetType.SNV_INDEL,
                ),
                *ClickHouseDictionary.for_dataset_type(DatasetType.SNV_INDEL),
            ],
        )
        stage_existing_project_partitions(
            table_name_builder,
            [
                'project_a',
                'project_b',
                'project_d',  # Partition does not exist already.
            ],
            ClickHouseTable.for_dataset_type_atomic_insert_entries_project_partitioned(
                DatasetType.SNV_INDEL,
            ),
        )
        staged_projects = client.execute(
            f"""
            SELECT DISTINCT project_guid FROM {STAGING_CLICKHOUSE_DATABASE}.`{table_name_builder.run_id_hash}/GRCh38/SNV_INDEL/entries`
            """,
        )
        self.assertCountEqual(
            [p[0] for p in staged_projects],
            ['project_a', 'project_b'],
        )
        staged_project_gt_stats = client.execute(
            f"""
            SELECT project_guid, key, sample_type, sum(het_samples), sum(hom_samples)
            FROM
            {STAGING_CLICKHOUSE_DATABASE}.`{table_name_builder.run_id_hash}/GRCh38/SNV_INDEL/project_gt_stats`
            GROUP BY project_guid, key, sample_type
            """,
        )
        self.assertCountEqual(
            staged_project_gt_stats,
            [
                ('project_a', 0, 'WES', 0, 1),
                ('project_a', 1, 'WGS', 1, 0),
                ('project_a', 2, 'WGS', 0, 1),
                ('project_a', 4, 'WES', 1, 0),
                ('project_a', 4, 'WGS', 0, 1),
                ('project_b', 1, 'WES', 1, 0),
                ('project_b', 3, 'WES', 0, 1),
                ('project_b', 4, 'WES', 0, 1),
                # project_gt_stats stages all projects, not just
                # those requested for loading.
                ('project_c', 4, 'WES', 0, 1),
                ('project_c', 5, 'WES', 0, 1),
            ],
        )
        delete_existing_families_from_staging_entries(
            table_name_builder,
            ['family_a1', 'family_a5', 'family_a6'],
        )
        staged_project_gt_stats = client.execute(
            f"""
            SELECT project_guid, key, sample_type, sum(het_samples), sum(hom_samples)
            FROM
            {STAGING_CLICKHOUSE_DATABASE}.`{table_name_builder.run_id_hash}/GRCh38/SNV_INDEL/project_gt_stats`
            GROUP BY project_guid, key, sample_type
            """,
        )
        self.assertCountEqual(
            staged_project_gt_stats,
            [
                ('project_a', 0, 'WES', 0, 0),
                ('project_a', 1, 'WGS', 1, 0),
                ('project_a', 2, 'WGS', 0, 1),
                ('project_a', 4, 'WES', 0, 0),
                ('project_a', 4, 'WGS', 0, 0),
                ('project_b', 1, 'WES', 1, 0),
                ('project_b', 3, 'WES', 0, 1),
                ('project_b', 4, 'WES', 0, 1),
                ('project_c', 4, 'WES', 0, 1),
                ('project_c', 5, 'WES', 0, 1),
            ],
        )
        insert_new_entries(table_name_builder)
        optimize_entries(
            table_name_builder,
        )
        staged_project_gt_stats = client.execute(
            f"""
            SELECT project_guid, key, sample_type, sum(het_samples), sum(hom_samples)
            FROM
            {STAGING_CLICKHOUSE_DATABASE}.`{table_name_builder.run_id_hash}/GRCh38/SNV_INDEL/project_gt_stats`
            GROUP BY project_guid, key, sample_type
            """,
        )
        self.assertCountEqual(
            staged_project_gt_stats,
            [
                ('project_a', 0, 'WES', 0, 0),
                ('project_a', 1, 'WGS', 1, 0),
                ('project_a', 2, 'WGS', 0, 1),
                ('project_a', 4, 'WES', 0, 0),
                ('project_a', 4, 'WGS', 0, 0),
                ('project_b', 1, 'WES', 1, 0),
                ('project_b', 3, 'WES', 0, 1),
                ('project_b', 4, 'WES', 0, 1),
                ('project_c', 4, 'WES', 0, 1),
                ('project_c', 5, 'WES', 0, 1),
                ('project_d', 0, 'WES', 0, 1),
                ('project_d', 4, 'WES', 1, 0),
            ],
        )
        refresh_materialized_views(
            table_name_builder,
            ClickHouseMaterializedView.for_dataset_type_atomic_insert_entries_refreshable(
                DatasetType.SNV_INDEL,
            ),
            staging=True,
        )
        replace_project_partitions(
            table_name_builder,
            ClickHouseTable.for_dataset_type_atomic_insert_entries_project_partitioned(
                DatasetType.SNV_INDEL,
            ),
            ['project_a', 'project_d'],
        )
        new_entries = client.execute(
            f"""
            SELECT COLUMNS('.*') EXCEPT(is_annotated_in_any_gene)
            REPLACE(
                bitmapToArray(geneId_ids) AS geneId_ids
            )
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries`
            """,
        )
        self.maxDiff = None
        self.assertCountEqual(
            new_entries,
            [
                (
                    0,
                    'project_b',
                    'family_b1',
                    123456789,
                    'WES',
                    [],
                    [('sample_b4', 'REF')],
                    1,
                ),
                (
                    1,
                    'project_b',
                    'family_b2',
                    123456789,
                    'WES',
                    [],
                    [('sample_b5', 'HET')],
                    1,
                ),
                (
                    2,
                    'project_b',
                    'family_b2',
                    123456789,
                    'WES',
                    [],
                    [('sample_b5', 'REF')],
                    1,
                ),
                (
                    3,
                    'project_b',
                    'family_b3',
                    133456789,
                    'WES',
                    [],
                    [('sample_b6', 'HOM')],
                    1,
                ),
                (
                    4,
                    'project_b',
                    'family_b3',
                    133456789,
                    'WES',
                    [],
                    [('sample_b6', 'HOM')],
                    1,
                ),
                (
                    1,
                    'project_a',
                    'family_a2',
                    123456789,
                    'WGS',
                    [],
                    [('sample_a2', 'HET')],
                    1,
                ),
                (
                    2,
                    'project_a',
                    'family_a3',
                    133456789,
                    'WGS',
                    [],
                    [('sample_a3', 'HOM')],
                    1,
                ),
                (
                    3,
                    'project_a',
                    'family_a4',
                    133456789,
                    'WES',
                    [],
                    [('sample_a4', 'REF')],
                    1,
                ),
                (
                    0,
                    'project_d',
                    'family_d1',
                    123456789,
                    'WES',
                    [],
                    [('sample_d1', 'REF'), ('sample_d11', 'HOM')],
                    1,
                ),
                (
                    3,
                    'project_d',
                    'family_d2',
                    123456789,
                    'WES',
                    [123, 12],
                    [('sample_d2', 'REF')],
                    1,
                ),
                (
                    4,
                    'project_d',
                    'family_d3',
                    123456789,
                    'WES',
                    [1],
                    [('sample_d3', 'HET')],
                    1,
                ),
                (
                    0,
                    'project_c',
                    'family_c1',
                    123456789,
                    'WES',
                    [1],
                    [('sample_c7', 'REF')],
                    1,
                ),
                (
                    3,
                    'project_c',
                    'family_c2',
                    123456789,
                    'WES',
                    [1],
                    [('sample_c8', 'REF')],
                    1,
                ),
                (
                    4,
                    'project_c',
                    'family_c3',
                    133456789,
                    'WES',
                    [1],
                    [('sample_c9', 'HOM')],
                    1,
                ),
                (
                    5,
                    'project_c',
                    'family_c4',
                    133456789,
                    'WES',
                    [1],
                    [('sample_c9', 'HOM')],
                    1,
                ),
            ],
        )
        existing_gt_stats = client.execute(
            f"""
            SELECT *
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats_dict`
            """,
        )
        self.assertCountEqual(
            existing_gt_stats,
            [],
        )
        exchange_tables(
            table_name_builder,
            ClickHouseTable.for_dataset_type_atomic_insert_entries_unpartitioned(
                DatasetType.SNV_INDEL,
            ),
        )
        new_gt_stats = client.execute(
            f"""
            SELECT *
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats_dict`
            """,
        )
        self.assertCountEqual(new_gt_stats, [])
        reload_dictionaries(
            table_name_builder,
            ClickHouseDictionary.for_dataset_type(DatasetType.SNV_INDEL),
        )
        new_gt_stats_post_reload = client.execute(
            f"""
            SELECT *
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats_dict`
            """,
        )
        self.assertEqual(
            new_gt_stats_post_reload,
            [
                (0, 2, 0),
                (1, 1, 1),
                (2, 0, 2),
                (3, 2, 0),
                (4, 5, 0),
                (5, 2, 0),
            ],
        )

    def test_load_complete_run_snv_indel(self):
        load_complete_run(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
            ['project_d'],
            ['family_d1', 'family_d2'],
        )
        client = get_clickhouse_client()
        project_gt_stats = client.execute(
            f"""
           SELECT project_guid, key, sample_type, sum(het_samples), sum(hom_samples)
           FROM
           {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/project_gt_stats`
           GROUP BY project_guid, key, sample_type
           """,
        )
        self.assertCountEqual(
            project_gt_stats,
            [
                ('project_d', 0, 'WES', 0, 1),
                ('project_d', 4, 'WES', 1, 0),
            ],
        )
        gt_stats = client.execute(
            f"""
           SELECT *
           FROM
           {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats`
           """,
        )
        self.assertCountEqual(
            gt_stats,
            [
                (0, 2, 0),
                (4, 1, 0),
            ],
        )
        gt_stats_dict = client.execute(
            f"""
           SELECT *
           FROM
           {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats_dict`
           """,
        )
        self.assertCountEqual(
            gt_stats_dict,
            [
                (0, 2, 0),
                (4, 1, 0),
            ],
        )
        annotations_memory = client.execute(
            f"""
           SELECT *
           FROM
           {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/annotations_memory`
           """,
        )
        self.assertCountEqual(
            annotations_memory,
            [
                (10, '1-3-A-C'),
                (11, '2-4-A-T'),
                (12, 'Y-9-A-C'),
                (13, 'M-2-C-G'),
            ],
        )
        annotations_disk = client.execute(
            f"""
           SELECT *
           FROM
           {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/annotations_disk`
           """,
        )
        self.assertCountEqual(
            annotations_disk,
            [
                (10, '1-3-A-C'),
                (11, '2-4-A-T'),
                (12, 'Y-9-A-C'),
                (13, 'M-2-C-G'),
            ],
        )

    def test_load_complete_gcnv(self):
        load_complete_run(
            ReferenceGenome.GRCh38,
            DatasetType.GCNV,
            TEST_RUN_ID,
            ['project_d'],
            ['family_d1', 'family_d2'],
        )
        client = get_clickhouse_client()
        annotations_disk_count = client.execute(
            f"""
           SELECT COUNT(*)
           FROM
           {Env.CLICKHOUSE_DATABASE}.`GRCh38/GCNV/annotations_memory`
           """,
        )[0][0]
        self.assertEqual(annotations_disk_count, 4)
        annotations_disk_count = client.execute(
            f"""
           SELECT COUNT(*)
           FROM
           {Env.CLICKHOUSE_DATABASE}.`GRCh38/GCNV/annotations_disk`
           """,
        )[0][0]
        self.assertEqual(annotations_disk_count, 4)
        entries_count = client.execute(
            f"""
           SELECT COUNT(*)
           FROM
           {Env.CLICKHOUSE_DATABASE}.`GRCh38/GCNV/entries`
           """,
        )[0][0]
        self.assertEqual(entries_count, 3)
