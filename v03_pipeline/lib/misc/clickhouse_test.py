import os
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from v03_pipeline.lib.misc.clickhouse import (
    ClickHouseTable,
    TableNameBuilder,
    create_staging_entries,
    delete_existing_families,
    direct_insert,
    dst_key_exists,
    get_clickhouse_client,
    insert_new_entries,
    max_src_key,
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
            CREATE TABLE IF NOT EXISTS {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries` (
                `key` UInt32,
                `project_guid` LowCardinality(String),
                `family_guid` String,
                `xpos` UInt64 CODEC(Delta(8), ZSTD(1)),
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
            ORDER BY (project_guid, family_guid)
            SETTINGS deduplicate_merge_projection_mode = 'rebuild';
            """,
        )
        client.execute(
            f"""
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats`
            (
                `project_guid` String,
                `key` UInt32,
                `ref_samples` AggregateFunction(sum, Int32),
            )
            ENGINE = AggregatingMergeTree
            PARTITION BY project_guid
            ORDER BY (project_guid, key)
            SETTINGS index_granularity = 8192
            """,
        )
        client.execute(
            f"""
            CREATE MATERIALIZED VIEW {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries_to_gt_stats`
            TO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/gt_stats`
            AS SELECT
                project_guid,
                key,
                sumState(toInt32(arrayCount(s -> (s.gt = 'REF'), calls) * sign)) AS ref_samples
            FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries`
            GROUP BY project_guid, key
            """,
        )
        client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/clinvar` (
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
            CREATE TABLE IF NOT EXISTS {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/key_lookup` (
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

    def tearDown(self):
        super().tearDown()
        client = get_clickhouse_client()
        client.execute(
            f"""
            DROP DATABASE {Env.CLICKHOUSE_DATABASE};
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
            'test.`GRCh38/SNV_INDEL/entries`',
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
                "gcs('https://storage.googleapis.com/mock_bucket/GRCh38/SNV_INDEL/runs/manual__2025-05-07T17-20-59.702114+00-00/new_entries.parquet/*.parquet', '', '', 'Parquet')",
            )

    def test_dst_key_exists(self):
        client = get_clickhouse_client()
        client.execute(
            f'INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/clinvar` (key) VALUES',
            [(1,), (10,), (7,)],
        )
        self.assertEqual(
            dst_key_exists(
                TableNameBuilder(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                ClickHouseTable.CLINVAR,
                1,
            ),
            True,
        )
        self.assertEqual(
            dst_key_exists(
                TableNameBuilder(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                ClickHouseTable.CLINVAR,
                2,
            ),
            False,
        )

    def test_max_src_key(self):
        df = pd.DataFrame({'key': [1, 2, 3, 4], 'value': ['a', 'b', 'c', 'd']})
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
        schema = pa.schema([('key', pa.int32()), ('value', pa.int64())])
        empty_data = {
            'key': pa.array([], type=pa.int32()),
            'value': pa.array([], type=pa.int64()),
        }
        table = pa.Table.from_pydict(empty_data, schema=schema)
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
        df = pd.read_parquet(
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        self.assertEqual(
            max_src_key(
                TableNameBuilder(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                ClickHouseTable.TRANSCRIPTS,
            ),
            4,
        )
        self.assertEqual(
            max_src_key(
                TableNameBuilder(
                    ReferenceGenome.GRCh38,
                    DatasetType.SNV_INDEL,
                    TEST_RUN_ID,
                ),
                ClickHouseTable.ANNOTATIONS_DISK,
            ),
            None,
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
                'key': [10, 11, 12, 13],
                'variantId': [
                    '1-3-A-C',
                    '2-4-A-T',
                    'Y-9-A-C',
                    'M-2-C-G',
                ],
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
        # Integration-style test that validates the first
        # few steps of the atomic entries insertion flow.
        client = get_clickhouse_client()
        client.execute(
            f'INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries` VALUES',
            [
                (0, 'project_a', 'family_a1', 123456789, [('sample_a1', 'REF')], 1),
                (1, 'project_a', 'family_a2', 123456789, [('sample_a2', 'HET')], 1),
                (2, 'project_a', 'family_a3', 133456789, [('sample_a3', 'HOM')], 1),
                (3, 'project_a', 'family_a4', 133456789, [('sample_a4', 'REF')], 1),
                (
                    4,
                    'project_a',
                    'family_a5',
                    133456789,
                    [('sample_a5', 'REF'), ('sample_a6', 'REF'), ('sample_a7', 'REF')],
                    1,
                ),
                (0, 'project_b', 'family_b1', 123456789, [('sample_b4', 'REF')], 1),
                (1, 'project_b', 'family_b2', 123456789, [('sample_b5', 'HET')], 1),
                (3, 'project_b', 'family_b3', 133456789, [('sample_b6', 'HOM')], 1),
                (0, 'project_c', 'family_c1', 123456789, [('sample_c7', 'REF')], 1),
                (3, 'project_c', 'family_c2', 123456789, [('sample_c8', 'REF')], 1),
                (4, 'project_c', 'family_c3', 133456789, [('sample_c9', 'HOM')], 1),
                (5, 'project_c', 'family_c4', 133456789, [('sample_c9', 'HOM')], 1),
            ],
        )
        table_name_builder = TableNameBuilder(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
        )
        create_staging_entries(table_name_builder)
        stage_existing_project_partitions(
            table_name_builder,
            [
                'project_a',
                'project_b',
                'project_d',  # Partition does not exist already.
            ],
        )
        ref_sample_count = client.execute(
            f"""
            SELECT key, sumMerge(ref_samples)
            FROM
            staging.`{TEST_RUN_ID}/GRCh38/SNV_INDEL/gt_stats`
            GROUP BY key
            """,
        )
        self.assertCountEqual(
            ref_sample_count,
            [(0, 2), (4, 3), (3, 1), (2, 0), (1, 0)],
        )
        delete_existing_families(
            table_name_builder,
            ['project_a'],
            ['family_a1', 'family_a5'],
        )
        ref_sample_count = client.execute(
            f"""
            SELECT key, sumMerge(ref_samples)
            FROM
            staging.`{TEST_RUN_ID}/GRCh38/SNV_INDEL/gt_stats`
            GROUP BY key
            """,
        )
        self.assertCountEqual(
            ref_sample_count,
            [(0, 1), (4, 0), (3, 1), (2, 0), (1, 0)],
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
                    'project_d1',
                    'project_d2',
                    'project_d3',
                ],
                'xpos': [
                    123456789,
                    123456789,
                    123456789,
                ],
                'calls': [
                    [('sample_d1', 0), ('sample_d11', 0)],
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
        ref_sample_count = client.execute(
            f"""
            SELECT key, sumMerge(ref_samples)
            FROM
            staging.`{TEST_RUN_ID}/GRCh38/SNV_INDEL/gt_stats`
            GROUP BY key
            """,
        )
        self.assertCountEqual(
            ref_sample_count,
            [(0, 3), (4, 0), (3, 2), (2, 0), (1, 0)],
        )
        replace_project_partitions(
            table_name_builder,
            ['project_d'],
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
                (0, 'project_a', 'family_a1', 123456789, [('sample_a1', 'REF')], 1),
                (1, 'project_a', 'family_a2', 123456789, [('sample_a2', 'HET')], 1),
                (2, 'project_a', 'family_a3', 133456789, [('sample_a3', 'HOM')], 1),
                (3, 'project_a', 'family_a4', 133456789, [('sample_a4', 'REF')], 1),
                (
                    4,
                    'project_a',
                    'family_a5',
                    133456789,
                    [('sample_a5', 'REF'), ('sample_a6', 'REF'), ('sample_a7', 'REF')],
                    1,
                ),
                (0, 'project_b', 'family_b1', 123456789, [('sample_b4', 'REF')], 1),
                (1, 'project_b', 'family_b2', 123456789, [('sample_b5', 'HET')], 1),
                (3, 'project_b', 'family_b3', 133456789, [('sample_b6', 'HOM')], 1),
                (
                    0,
                    'project_d',
                    'project_d1',
                    123456789,
                    [('sample_d1', 'REF'), ('sample_d11', 'REF')],
                    1,
                ),
                (3, 'project_d', 'project_d2', 123456789, [('sample_d2', 'REF')], 1),
                (4, 'project_d', 'project_d3', 123456789, [('sample_d3', 'HET')], 1),
                (0, 'project_c', 'family_c1', 123456789, [('sample_c7', 'REF')], 1),
                (3, 'project_c', 'family_c2', 123456789, [('sample_c8', 'REF')], 1),
                (4, 'project_c', 'family_c3', 133456789, [('sample_c9', 'HOM')], 1),
                (5, 'project_c', 'family_c4', 133456789, [('sample_c9', 'HOM')], 1),
            ],
        )
