import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from v03_pipeline.lib.misc.clickhouse import (
    ClickhouseTable,
    clickhouse_insert_table_fn,
    direct_insert,
    dst_key_exists,
    get_clickhouse_client,
    max_src_key,
)
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    new_transcripts_parquet_path,
    new_variants_parquet_path,
    runs_path,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_RUN_ID = 'manual__2024-04-03'


class ClickhouseTest(MockedDatarootTestCase):
    def setUp(self):
        super().setUp()
        client = get_clickhouse_client()
        client.execute(f"""
            CREATE DATABASE IF NOT EXISTS {Env.CLICKHOUSE_DATABASE};
        """)
        base_path = runs_path(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
        )
        os.makedirs(os.path.join(base_path, TEST_RUN_ID), exist_ok=True)

    def tearDown(self):
        super().tearDown()
        client = get_clickhouse_client()
        client.execute(f"""
            DROP DATABASE {Env.CLICKHOUSE_DATABASE};
        """)

    def test_get_clickhouse_client(self):
        client = get_clickhouse_client()
        result = client.execute('SELECT 1')
        self.assertEqual(result[0][0], 1)

    def test_clickhouse_insert_table_fn(self):
        path = 'gcs://my-bucket/my-file.txt'
        self.assertEqual(
            clickhouse_insert_table_fn(path),
            "gcs('https://storage.googleapis.com/my-bucket/my-file.txt', '', '', 'Parquet')",
        )
        path = '/var/seqr/my-bucket/my-file.txt'
        self.assertEqual(
            clickhouse_insert_table_fn(path),
            "file('/var/seqr/my-bucket/my-file.txt', 'Parquet')",
        )

    def test_dst_key_exists(self):
        client = get_clickhouse_client()
        client.execute(f"""
            CREATE TABLE IF NOT EXISTS {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/clinvar` (
                key UInt32
            ) ENGINE = MergeTree()
            ORDER BY key
        """)
        client.execute(
            f'INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/clinvar` (key) VALUES',
            [(1,), (10,), (7,)],
        )
        self.assertEqual(
            dst_key_exists(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ClickhouseTable.CLINVAR,
                1,
            ),
            True,
        )
        self.assertEqual(
            dst_key_exists(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                ClickhouseTable.CLINVAR,
                2,
            ),
            False,
        )

    def test_max_src_key(self):
        df = pd.DataFrame({'key': [1, 2, 3, 4], 'value': ['a', 'b', 'c', 'd']})
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        schema = pa.schema([('key', pa.int32()), ('value', pa.int64())])
        empty_data = {
            'key': pa.array([], type=pa.int32()),
            'value': pa.array([], type=pa.int64()),
        }
        table = pa.Table.from_pydict(empty_data, schema=schema)
        pq.write_table(
            table,
            new_variants_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
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
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
                ClickhouseTable.TRANSCRIPTS,
            ),
            4,
        )
        self.assertEqual(
            max_src_key(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
                ClickhouseTable.ANNOTATIONS_DISK,
            ),
            None,
        )

    def test_direct_insert(self):
        client = get_clickhouse_client()
        df = pd.DataFrame({'key': [1, 2, 3, 4], 'transcripts': ['a', 'b', 'c', 'd']})
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            new_transcripts_parquet_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                TEST_RUN_ID,
            ),
        )
        client.execute(f"""
            CREATE TABLE IF NOT EXISTS {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts` (
                key UInt32,
                transcripts String
            ) ENGINE = EmbeddedRocksDB()
            PRIMARY KEY `key`
            ORDER BY key
        """)
        client.execute(
            f'INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts` VALUES',
            [(1, 'a'), (10, 'b'), (7, 'c')],
        )
        direct_insert(
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            TEST_RUN_ID,
            ClickhouseTable.TRANSCRIPTS,
        )
        ret = client.execute(
            f'SELECT * FROM {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/transcripts`',
        )
        self.assertEqual(
            ret, [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (7, 'c'), (10, 'b')]
        )
