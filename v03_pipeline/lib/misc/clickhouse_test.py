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
    atomic_entries_insert,
    create_staging_non_table_entities,
    create_staging_tables,
    delete_existing_families_from_staging_entries,
    direct_insert,
    exchange_entity,
    get_clickhouse_client,
    insert_new_entries,
    optimize_entries,
    refresh_staged_gt_stats,
    reload_staged_gt_stats_dict,
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
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/clinvar` (
                key UInt32
            ) ENGINE = MergeTree()
            ORDER BY key
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
                    USER {Env.CLICKHOUSE_USER} PASSWORD {Env.CLICKHOUSE_PASSWORD or "''"}
                    DB {Env.CLICKHOUSE_DATABASE} TABLE `GRCh38/SNV_INDEL/gt_stats`
                )
            )
            LIFETIME(0)
            LAYOUT(FLAT(MAX_ARRAY_SIZE 10000))
            """,
        )
        base_path = runs_path(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
        )
        os.makedirs(os.path.join(base_path, TEST_RUN_ID), exist_ok=True)

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
                "gcs('https://storage.googleapis.com/mock_bucket/v3.1/GRCh38/SNV_INDEL/runs/manual__2025-05-07T17-20-59.702114+00-00/new_entries.parquet/*.parquet', '', '', 'Parquet')",
            )

    def test_direct_insert(self):
        client = get_clickhouse_client()
        df = pd.DataFrame({'key': [1, 2, 3, 4], 'transcripts': ['a', 'b', 'c', 'd']})
        table = pa.Table.from_pandas(df)
        os.makedirs(
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        pq.write_table(
            table,
            os.path.join(
                new_transcripts_parquet_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                'test.parquet',
            ),
        )
        client.execute(
            f'INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts` VALUES',
            [(1, 'a'), (10, 'b'), (7, 'c')],
        )
        direct_insert(
            ClickHouseTable.TRANSCRIPTS,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
        )
        ret = client.execute(
            f'SELECT * FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts`',
        )
        self.assertEqual(
            ret,
            [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (7, 'c'), (10, 'b')],
        )

        # ensure multiple calls are idempotent
        direct_insert(
            ClickHouseTable.TRANSCRIPTS,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
        )
        ret = client.execute(
            f'SELECT * FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts`',
        )
        self.assertEqual(
            ret,
            [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (7, 'c'), (10, 'b')],
        )

    def test_direct_insert_key_lookup(self):
        client = get_clickhouse_client()
        df = pd.DataFrame(
            {
                'variantId': [
                    '1-3-A-C',
                    '2-4-A-T',
                    'Y-9-A-C',
                    'M-2-C-G',
                ],
                'key': [10, 11, 12, 13],
            },
        )
        table = pa.Table.from_pandas(df)
        os.makedirs(
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        pq.write_table(
            table,
            os.path.join(
                new_variants_parquet_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                'test.parquet',
            ),
        )
        client.execute(
            f'INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/key_lookup` VALUES',
            [('1-123-A-C', 1), ('2-234-C-T', 2), ('M-345-C-G', 3)],
        )
        direct_insert(
            ClickHouseTable.KEY_LOOKUP,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
        )
        ret = client.execute(
            f'SELECT * FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/key_lookup` ORDER BY variantId ASC',
        )
        self.assertEqual(
            ret,
            [
                ('1-123-A-C', 1),
                ('1-3-A-C', 10),
                ('2-234-C-T', 2),
                ('2-4-A-T', 11),
                ('M-2-C-G', 13),
                ('M-345-C-G', 3),
                ('Y-9-A-C', 12),
            ],
        )

    def test_entries_insert_flow(self):
        # Tests individual components of the atomic_entries_insert
        # to validate the state after each step.
        client = get_clickhouse_client()
        client.execute(
            f'INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries` VALUES',
            [
                (
                    0,
                    'project_a',
                    'family_a1',
                    123456789,
                    'WES',
                    [('sample_a1', 'HOM')],
                    1,
                ),
                (
                    1,
                    'project_a',
                    'family_a2',
                    123456789,
                    'WGS',
                    [('sample_a2', 'HET')],
                    1,
                ),
                (
                    2,
                    'project_a',
                    'family_a3',
                    133456789,
                    'WGS',
                    [('sample_a3', 'HOM')],
                    1,
                ),
                (
                    3,
                    'project_a',
                    'family_a4',
                    133456789,
                    'WES',
                    [('sample_a4', 'REF')],
                    1,
                ),
                (
                    4,
                    'project_a',
                    'family_a5',
                    133456789,
                    'WES',
                    [('sample_a5', 'REF'), ('sample_a6', 'HET'), ('sample_a7', 'REF')],
                    1,
                ),
                (
                    4,
                    'project_a',
                    'family_a6',
                    133456789,
                    'WGS',
                    [('sample_a8', 'HOM')],
                    1,
                ),
                (
                    0,
                    'project_b',
                    'family_b1',
                    123456789,
                    'WES',
                    [('sample_b4', 'REF')],
                    1,
                ),
                (
                    1,
                    'project_b',
                    'family_b2',
                    123456789,
                    'WES',
                    [('sample_b5', 'HET')],
                    1,
                ),
                (
                    2,
                    'project_b',
                    'family_b2',
                    123456789,
                    'WES',
                    [('sample_b5', 'REF')],
                    1,
                ),
                (
                    3,
                    'project_b',
                    'family_b3',
                    133456789,
                    'WES',
                    [('sample_b6', 'HOM')],
                    1,
                ),
                (
                    4,
                    'project_b',
                    'family_b3',
                    133456789,
                    'WES',
                    [('sample_b6', 'HOM')],
                    1,
                ),
                (
                    0,
                    'project_c',
                    'family_c1',
                    123456789,
                    'WES',
                    [('sample_c7', 'REF')],
                    1,
                ),
                (
                    3,
                    'project_c',
                    'family_c2',
                    123456789,
                    'WES',
                    [('sample_c8', 'REF')],
                    1,
                ),
                (
                    4,
                    'project_c',
                    'family_c3',
                    133456789,
                    'WES',
                    [('sample_c9', 'HOM')],
                    1,
                ),
                (
                    5,
                    'project_c',
                    'family_c4',
                    133456789,
                    'WES',
                    [('sample_c9', 'HOM')],
                    1,
                ),
            ],
        )
        table_name_builder = TableNameBuilder(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
        )
        create_staging_tables(
            table_name_builder,
            [
                ClickHouseTable.ENTRIES,
                ClickHouseTable.PROJECT_GT_STATS,
                ClickHouseTable.GT_STATS,
            ],
        )
        create_staging_non_table_entities(
            table_name_builder,
            [
                ClickHouseMaterializedView.ENTRIES_TO_PROJECT_GT_STATS_MV,
                ClickHouseMaterializedView.PROJECT_GT_STATS_TO_GT_STATS_MV,
                ClickHouseDictionary.GT_STATS_DICT,
            ],
        )
        stage_existing_project_partitions(
            table_name_builder,
            DatasetType.SNV_INDEL,
            [
                'project_a',
                'project_b',
                'project_d',  # Partition does not exist already.
            ],
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
                (
                    'calls',
                    pa.list_(
                        pa.struct([('sampleId', pa.string()), ('gt', pa.int64())]),
                    ),
                ),
                ('sign', pa.int64()),
            ],
        )
        table = pa.Table.from_pandas(df, schema=schema)
        os.makedirs(
            new_entries_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        pq.write_table(
            table,
            os.path.join(
                new_entries_parquet_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                'test.parquet',
            ),
        )
        insert_new_entries(table_name_builder)
        optimize_entries(
            table_name_builder,
            ['project_a', 'project_d'],
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
        refresh_staged_gt_stats(table_name_builder)
        staged_gt_stats = client.execute(
            f"""
            SELECT * FROM {STAGING_CLICKHOUSE_DATABASE}.`{table_name_builder.run_id_hash}/GRCh38/SNV_INDEL/gt_stats`
            """,
        )
        self.assertEqual(
            staged_gt_stats,
            [(0, 2, 0), (1, 1, 1), (2, 0, 2), (3, 2, 0), (4, 5, 0), (5, 2, 0)],
        )
        staged_gt_stats_dict = client.execute(
            f"""
            SELECT * FROM {STAGING_CLICKHOUSE_DATABASE}.`{table_name_builder.run_id_hash}/GRCh38/SNV_INDEL/gt_stats_dict`
            """,
        )
        self.assertEqual(
            staged_gt_stats_dict,
            [],
        )
        replace_project_partitions(
            table_name_builder,
            ['project_a', 'project_d'],
            [
                ClickHouseTable.ENTRIES,
                ClickHouseTable.PROJECT_GT_STATS,
            ],
        )
        new_entries = client.execute(
            f"""
            SELECT *
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries`
            """,
        )
        self.assertCountEqual(
            new_entries,
            [
                (
                    0,
                    'project_b',
                    'family_b1',
                    123456789,
                    'WES',
                    [('sample_b4', 'REF')],
                    1,
                ),
                (
                    1,
                    'project_b',
                    'family_b2',
                    123456789,
                    'WES',
                    [('sample_b5', 'HET')],
                    1,
                ),
                (
                    2,
                    'project_b',
                    'family_b2',
                    123456789,
                    'WES',
                    [('sample_b5', 'REF')],
                    1,
                ),
                (
                    3,
                    'project_b',
                    'family_b3',
                    133456789,
                    'WES',
                    [('sample_b6', 'HOM')],
                    1,
                ),
                (
                    4,
                    'project_b',
                    'family_b3',
                    133456789,
                    'WES',
                    [('sample_b6', 'HOM')],
                    1,
                ),
                (
                    1,
                    'project_a',
                    'family_a2',
                    123456789,
                    'WGS',
                    [('sample_a2', 'HET')],
                    1,
                ),
                (
                    2,
                    'project_a',
                    'family_a3',
                    133456789,
                    'WGS',
                    [('sample_a3', 'HOM')],
                    1,
                ),
                (
                    3,
                    'project_a',
                    'family_a4',
                    133456789,
                    'WES',
                    [('sample_a4', 'REF')],
                    1,
                ),
                (
                    0,
                    'project_d',
                    'family_d1',
                    123456789,
                    'WES',
                    [('sample_d1', 'REF'), ('sample_d11', 'HOM')],
                    1,
                ),
                (
                    3,
                    'project_d',
                    'family_d2',
                    123456789,
                    'WES',
                    [('sample_d2', 'REF')],
                    1,
                ),
                (
                    4,
                    'project_d',
                    'family_d3',
                    123456789,
                    'WES',
                    [('sample_d3', 'HET')],
                    1,
                ),
                (
                    0,
                    'project_c',
                    'family_c1',
                    123456789,
                    'WES',
                    [('sample_c7', 'REF')],
                    1,
                ),
                (
                    3,
                    'project_c',
                    'family_c2',
                    123456789,
                    'WES',
                    [('sample_c8', 'REF')],
                    1,
                ),
                (
                    4,
                    'project_c',
                    'family_c3',
                    133456789,
                    'WES',
                    [('sample_c9', 'HOM')],
                    1,
                ),
                (
                    5,
                    'project_c',
                    'family_c4',
                    133456789,
                    'WES',
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
        exchange_entity(
            table_name_builder,
            ClickHouseTable.GT_STATS,
        )
        reload_staged_gt_stats_dict(
            table_name_builder,
        )
        new_gt_stats = client.execute(
            f"""
            SELECT *
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats_dict`
            """,
        )
        self.assertCountEqual(new_gt_stats, [])
        exchange_entity(
            table_name_builder,
            ClickHouseDictionary.GT_STATS_DICT,
        )
        new_gt_stats_post_exchange = client.execute(
            f"""
            SELECT *
            FROM
            {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats_dict`
            """,
        )
        self.assertEqual(
            new_gt_stats_post_exchange,
            [
                (0, 2, 0),
                (1, 1, 1),
                (2, 0, 2),
                (3, 2, 0),
                (4, 5, 0),
                (5, 2, 0),
            ],
        )

        staging_tables = client.execute(
            """
            SELECT create_table_query FROM system.tables
            WHERE
            database = %(database)s
            """,
            {'database': STAGING_CLICKHOUSE_DATABASE},
        )
        self.assertEqual(
            len(staging_tables),
            6,
        )
        self.assertEqual(
            next(iter([s[0] for s in staging_tables if 'DICTIONARY' in s[0]])).strip(),
            # important test!  Ensuring that the staging dictionary points to the production gt_stats table.
            f"""
            CREATE DICTIONARY staging.`{table_name_builder.run_id_hash}/GRCh38/SNV_INDEL/gt_stats_dict` (`key` UInt32, `ac_wes` UInt16, `ac_wgs` UInt16) PRIMARY KEY key SOURCE(CLICKHOUSE(USER {Env.CLICKHOUSE_USER} PASSWORD '[HIDDEN]' DB {Env.CLICKHOUSE_DATABASE} TABLE `GRCh38/SNV_INDEL/gt_stats`)) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT(MAX_ARRAY_SIZE 10000))
            """.strip(),
        )

    def test_atomic_entries_insert(self):
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
                'calls': [
                    [('sample_d1', 0), ('sample_d11', 1)],
                    [('sample_d2', 0)],
                    [('sample_d3', 2)],
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
                (
                    'calls',
                    pa.list_(
                        pa.struct([('sampleId', pa.string()), ('gt', pa.int64())]),
                    ),
                ),
                ('sign', pa.int64()),
            ],
        )
        table = pa.Table.from_pandas(df, schema=schema)
        os.makedirs(
            new_entries_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        pq.write_table(
            table,
            os.path.join(
                new_entries_parquet_path(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                'test.parquet',
            ),
        )
        atomic_entries_insert(
            ClickHouseTable.ENTRIES,
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
                ('project_d', 0, 'WES', 1, 0),
                ('project_d', 4, 'WES', 0, 1),
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
                (0, 1, 0),
                (4, 2, 0),
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
                (0, 1, 0),
                (4, 2, 0),
            ],
        )
