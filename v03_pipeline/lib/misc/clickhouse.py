import os
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from clickhouse_driver import Client

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    new_clinvar_variants_parquet_path,
    new_entries_parquet_path,
    new_transcripts_parquet_path,
    new_variants_parquet_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
)

logger = get_logger(__name__)

GOOGLE_XML_API_PATH = 'https://storage.googleapis.com/'
KEY = 'key'
STAGING_CLICKHOUSE_DATABASE = 'staging'
VARIANT_ID = 'variantId'


class ClickHouseTable(StrEnum):
    ANNOTATIONS_DISK = 'annotations_disk'
    ANNOTATIONS_MEMORY = 'annotations_memory'
    CLINVAR = 'clinvar'
    KEY_LOOKUP = 'key_lookup'
    TRANSCRIPTS = 'transcripts'
    ENTRIES = 'entries'
    GT_STATS = 'gt_stats'
    ENTRIES_TO_GT_STATS = 'entries_to_gt_stats'

    @property
    def src_path_fn(self) -> Callable:
        return {
            ClickHouseTable.ANNOTATIONS_DISK: new_variants_parquet_path,
            ClickHouseTable.ANNOTATIONS_MEMORY: new_variants_parquet_path,
            ClickHouseTable.CLINVAR: new_clinvar_variants_parquet_path,
            ClickHouseTable.KEY_LOOKUP: new_variants_parquet_path,
            ClickHouseTable.TRANSCRIPTS: new_transcripts_parquet_path,
            ClickHouseTable.ENTRIES: new_entries_parquet_path,
        }[self]

    def should_load(self, reference_genome: ReferenceGenome, dataset_type: DatasetType):
        if self in {ClickHouseTable.GT_STATS, ClickHouseTable.ENTRIES_TO_GT_STATS}:
            return False
        if self == ClickHouseTable.CLINVAR:
            return (
                ReferenceDataset.clinvar
                in BaseReferenceDataset.for_reference_genome_dataset_type(
                    reference_genome,
                    dataset_type,
                )
            )
        return True

    @property
    def key_field(self):
        return VARIANT_ID if self == ClickHouseTable.KEY_LOOKUP else KEY

    @property
    def select_fields(self):
        return f'{VARIANT_ID}, {KEY}' if self == ClickHouseTable.KEY_LOOKUP else '*'

    @property
    def insert(self) -> Callable:
        return (
            direct_insert if self != ClickHouseTable.ENTRIES else atomic_entries_insert
        )


class ClickHouseDictionary(StrEnum):
    GT_STATS_DICT = 'gt_stats_dict'


ClickHouseEntity = ClickHouseDictionary | ClickHouseTable


@dataclass
class TableNameBuilder:
    reference_genome: ReferenceGenome
    dataset_type: DatasetType
    run_id: str

    def dst_table(self, clickhouse_entity: ClickHouseEntity):
        return f'{Env.CLICKHOUSE_DATABASE}.`{self.reference_genome.value}/{self.dataset_type.value}/{clickhouse_entity.value}`'

    def staging_dst_table(self, clickhouse_table: ClickHouseTable):
        return f'{STAGING_CLICKHOUSE_DATABASE}.`{self.run_id}/{self.reference_genome.value}/{self.dataset_type.value}/{clickhouse_table.value}`'

    def src_table(self, clickhouse_table: ClickHouseTable):
        path = os.path.join(
            clickhouse_table.src_path_fn(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
            '*.parquet',
        )
        if path.startswith('gs://'):
            return f"gcs('{path.replace('gs://', GOOGLE_XML_API_PATH)}', '{Env.CLICKHOUSE_GCS_HMAC_KEY}', '{Env.CLICKHOUSE_GCS_HMAC_SECRET}', 'Parquet')"
        return f"file('{path}', 'Parquet')"


@retry()
def drop_staging_db():
    logger.info('Dropping all staging tables')
    client = get_clickhouse_client()
    client.execute(f'DROP DATABASE IF EXISTS {STAGING_CLICKHOUSE_DATABASE};')


def dst_key_exists(
    table_name_builder: TableNameBuilder,
    clickhouse_table: ClickHouseTable,
    key: int | str,
) -> int:
    client = get_clickhouse_client()
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM {table_name_builder.dst_table(clickhouse_table)}
            WHERE {clickhouse_table.key_field} = %(key)s
        )
        """
    return client.execute(query, {'key': key})[0][0]


def max_src_key(
    table_name_builder: TableNameBuilder,
    clickhouse_table: ClickHouseTable,
) -> int:
    client = get_clickhouse_client()
    return client.execute(
        f"""
        SELECT max({clickhouse_table.key_field}) FROM {table_name_builder.src_table(clickhouse_table)}
        """,
    )[0][0]


def create_staging_entries(
    table_name_builder: TableNameBuilder,
) -> None:
    client = get_clickhouse_client()
    client.execute(
        f"""
        CREATE DATABASE {STAGING_CLICKHOUSE_DATABASE}
        """,
    )
    for clickhouse_table in [
        ClickHouseTable.ENTRIES,
        ClickHouseTable.GT_STATS,
    ]:
        client.execute(
            f"""
            CREATE
            TABLE {table_name_builder.staging_dst_table(clickhouse_table)}
            AS {table_name_builder.dst_table(clickhouse_table)}
            """,
        )

    # Special system logic for the materialized view sourced from
    # the reference script: https://github.com/ClickHouse/examples/blob/cc4287fe759e67fd7af0ab3a5a79b42ac0c5a969/large_data_loads/src/worker.py#L523
    # CREATE MATERIALIZED VIEW AS does not work for the incremental view, and requires manipulating
    # the source view's create statement.
    create_view_statement = client.execute(
        """
        SELECT create_table_query FROM system.tables
        WHERE
        database = %(database)s
        AND name = %(name)s
        """,
        {
            'database': Env.CLICKHOUSE_DATABASE,
            'name': table_name_builder.dst_table(ClickHouseTable.ENTRIES_TO_GT_STATS)
            .split('.')[1]
            .replace('`', ''),
        },
    )[0][0]
    client.execute(
        create_view_statement.replace(
            Env.CLICKHOUSE_DATABASE,
            STAGING_CLICKHOUSE_DATABASE,
        ).replace(
            table_name_builder.reference_genome,
            f'{table_name_builder.run_id}/{table_name_builder.reference_genome.value}',
        ),
    )


def stage_existing_project_partitions(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
):
    client = get_clickhouse_client()
    for clickhouse_table in [
        ClickHouseTable.ENTRIES,
        ClickHouseTable.GT_STATS,
    ]:
        for project_guid in project_guids:
            # Note that ClickHouse successfully handles the case where the project
            # does not already exist in the dst table.  We simply attach an empty partition!
            client.execute(
                f"""
                ALTER TABLE {table_name_builder.staging_dst_table(clickhouse_table)}
                ATTACH PARTITION %(project_guid)s FROM {table_name_builder.dst_table(clickhouse_table)}
                """,
                {'project_guid': project_guid},
            )


def delete_existing_families(
    table_name_builder: TableNameBuilder,
    family_guids: list[str],
) -> None:
    client = get_clickhouse_client()
    client.execute(
        f"""
        INSERT INTO {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)}
        SELECT COLUMNS('.*') EXCEPT(sign), -1 as sign
        FROM {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)}
        WHERE family_guid in %(family_guids)s
        """,
        {'family_guids': family_guids},
    )


def insert_new_entries(
    table_name_builder: TableNameBuilder,
) -> None:
    client = get_clickhouse_client()
    client.execute(
        f"""
        INSERT INTO {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)}
        SELECT *
        FROM {table_name_builder.src_table(ClickHouseTable.ENTRIES)}
        """,
    )
    # ClickHouse docs generally recommend against running OPTIMIZE TABLE FINAL.
    # However, forcing the merge to happen at load time should make the
    # application layer free of eventual consistency bugs.
    for clickhouse_table in [
        ClickHouseTable.ENTRIES,
        ClickHouseTable.GT_STATS,
    ]:
        client.execute(
            f"""
            OPTIMIZE TABLE {table_name_builder.staging_dst_table(clickhouse_table)} FINAL
            """,
        )


def replace_project_partitions(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
) -> None:
    client = get_clickhouse_client()
    for clickhouse_table in [
        ClickHouseTable.ENTRIES,
        ClickHouseTable.GT_STATS,
    ]:
        for project_guid in project_guids:
            client.execute(
                f"""
                ALTER TABLE {table_name_builder.dst_table(clickhouse_table)}
                REPLACE PARTITION %(project_guid)s FROM {table_name_builder.staging_dst_table(clickhouse_table)}
                """,
                {'project_guid': project_guid},
            )


def refresh_dictionary(
    table_name_builder: TableNameBuilder,
    clickhouse_dictionary: ClickHouseDictionary,
) -> None:
    client = get_clickhouse_client()
    client.execute(
        f"""
        SYSTEM RELOAD DICTIONARY {table_name_builder.dst_table(clickhouse_dictionary)}
        """,
    )


@retry()
def direct_insert(
    clickhouse_table: ClickHouseTable,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    *_,
) -> None:
    table_name_builder = TableNameBuilder(
        reference_genome,
        dataset_type,
        run_id,
    )
    key = max_src_key(
        table_name_builder,
        clickhouse_table,
    )
    if not key:
        msg = f'Skipping direct insert of {table_name_builder.dst_table(clickhouse_table)} since src table is empty'
        logger.info(msg)
        return
    if dst_key_exists(
        table_name_builder,
        clickhouse_table,
        key,
    ):
        msg = f'Skipping direct insert of {table_name_builder.dst_table(clickhouse_table)} since {clickhouse_table.key_field}={key} already exists'
        logger.info(msg)
        return
    client = get_clickhouse_client()
    client.execute(
        f"""
        INSERT INTO {table_name_builder.dst_table(clickhouse_table)}
        SELECT {clickhouse_table.select_fields}
        FROM {table_name_builder.src_table(clickhouse_table)}
        ORDER BY {clickhouse_table.key_field} ASC
        """,
    )


@retry()
def atomic_entries_insert(
    _clickhouse_table: ClickHouseTable,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    project_guids: list[str],
    family_guids: list[str],
    *_,
) -> None:
    table_name_builder = TableNameBuilder(
        reference_genome,
        dataset_type,
        run_id,
    )
    drop_staging_db()
    create_staging_entries(table_name_builder)
    stage_existing_project_partitions(
        table_name_builder,
        project_guids,
    )
    delete_existing_families(
        table_name_builder,
        family_guids,
    )
    insert_new_entries(
        table_name_builder,
    )
    replace_project_partitions(
        table_name_builder,
        project_guids,
    )
    refresh_dictionary(
        table_name_builder,
        ClickHouseDictionary.GT_STATS_DICT,
    )
    drop_staging_db()


def get_clickhouse_client() -> Client:
    return Client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        user=Env.CLICKHOUSE_USER,
        **{'password': Env.CLICKHOUSE_PASSWORD} if Env.CLICKHOUSE_PASSWORD else {},
    )
