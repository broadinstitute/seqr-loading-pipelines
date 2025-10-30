import unittest

from v03_pipeline.ops.repartion_clickhouse_grch38_snv_indel import main

from v03_pipeline.lib.core.environment import Env
from v03_pipeline.lib.misc.clickhouse import get_clickhouse_client


class RepartitionGRCh38SnvIndelTest(unittest.TestCase):
    def setUp(self):
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
            CREATE TABLE {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries` (
                `key` UInt32,
                `project_guid` LowCardinality(String),
                `family_guid` String,
                `is_annotated_in_any_gene` Boolean,
                `sign` Int8,
                PROJECTION xpos_projection
                (
                    SELECT *
                    ORDER BY is_annotated_in_any_gene
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
            INSERT INTO {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/entries`
            VALUES
            (0, 'project_a', 'family_a1', 0,  1),
            (1, 'project_a', 'family_a2', 0,  1),
            (2, 'project_a', 'family_a3', 0,  1),
            (3, 'project_a', 'family_a4', 0,  1),
            (4, 'project_a', 'family_a5', 1,  1),
            (4, 'project_a', 'family_a6', 0,  1),
            (0, 'project_b', 'family_b1', 0,  1),
            (1, 'project_b', 'family_b2', 0,  1),
            (2, 'project_b', 'family_b2', 0,  1),
            (3, 'project_b', 'family_b3', 0,  1),
            (4, 'project_b', 'family_b3', 0,  1),
            (0, 'project_c', 'family_c1', 1,  1),
            (3, 'project_c', 'family_c2', 1,  1),
            (4, 'project_c', 'family_c3', 1,  1),
            (5, 'project_c', 'family_c4', 1,  1)
            """,
        )
        client.execute(
            f"""
            CREATE DICTIONARY {Env.CLICKHOUSE_DATABASE}.`GRCh38/SNV_INDEL/project_partitions_dict`
            (
                `project_guid` String,
                `n_partitions` UInt8
            )
            PRIMARY KEY project_guid
            SOURCE(
                CLICKHOUSE(
                    USER {Env.CLICKHOUSE_WRITER_USER} PASSWORD {Env.CLICKHOUSE_WRITER_PASSWORD}
                    DB {Env.CLICKHOUSE_DATABASE} QUERY 'SELECT project_guid, 3 FROM `GRCh38/SNV_INDEL/entries`'
                )
            )
            LIFETIME(0)
            LAYOUT(FLAT(MAX_ARRAY_SIZE 10000))
            """,
        )

    def test_main_all_projects(self):
        main(1, [])

    def test_main_one_project(self):
        main(1, ['project_a'])
