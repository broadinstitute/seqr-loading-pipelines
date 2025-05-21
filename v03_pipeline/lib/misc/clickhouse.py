import functools
import hashlib
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
from v03_pipeline.lib.tasks.clickhouse_migration.constants import (
    ClickHouseMigrationType,
)

logger = get_logger(__name__)

GOOGLE_XML_API_PATH = 'https://storage.googleapis.com/'
KEY = 'key'
REDACTED = 'REDACTED'
STAGING_CLICKHOUSE_DATABASE = 'staging'
VARIANT_ID = 'variantId'


class ClickHouseTable(StrEnum):
    ANNOTATIONS_DISK = 'annotations_disk'
    ANNOTATIONS_MEMORY = 'annotations_memory'
    CLINVAR = 'clinvar'
    KEY_LOOKUP = 'key_lookup'
    TRANSCRIPTS = 'transcripts'
    ENTRIES = 'entries'
    PROJECT_GT_STATS = 'project_gt_stats'
    GT_STATS = 'gt_stats'

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

    def should_load(
        self,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
        migration_type: ClickHouseMigrationType | None,
    ):
        if migration_type:
            if migration_type == ClickHouseMigrationType.PROJECT_ENTRIES:
                return self == ClickHouseTable.ENTRIES
            if migration_type == ClickHouseMigrationType.VARIANTS:
                if self == ClickHouseTable.CLINVAR:
                    return (
                        ReferenceDataset.clinvar
                        in BaseReferenceDataset.for_reference_genome_dataset_type(
                            reference_genome,
                            dataset_type,
                        )
                    )
                return self in {
                    ClickHouseTable.ANNOTATIONS_DISK,
                    ClickHouseTable.ANNOTATIONS_MEMORY,
                    ClickHouseTable.KEY_LOOKUP,
                    ClickHouseTable.TRANSCRIPTS,
                }
            msg = f'Unhandled ClickHouseMigrationType: {migration_type.value}'
            raise ValueError(
                msg,
            )
        if self == ClickHouseTable.CLINVAR:
            return (
                ReferenceDataset.clinvar
                in BaseReferenceDataset.for_reference_genome_dataset_type(
                    reference_genome,
                    dataset_type,
                )
            )
        return self in {
            ClickHouseTable.ANNOTATIONS_DISK,
            ClickHouseTable.ANNOTATIONS_MEMORY,
            ClickHouseTable.KEY_LOOKUP,
            ClickHouseTable.TRANSCRIPTS,
            ClickHouseTable.ENTRIES,
        }

    @property
    def key_field(self):
        return VARIANT_ID if self == ClickHouseTable.KEY_LOOKUP else KEY

    @property
    def select_fields(self):
        return f'{VARIANT_ID}, {KEY}' if self == ClickHouseTable.KEY_LOOKUP else '*'

    @property
    def insert(self) -> Callable:
        return (
            functools.partial(direct_insert, clickhouse_table=self)
            if self != ClickHouseTable.ENTRIES
            else functools.partial(atomic_entries_insert, _clickhouse_table=self)
        )


class ClickHouseDictionary(StrEnum):
    GT_STATS_DICT = 'gt_stats_dict'


class ClickHouseMaterializedView(StrEnum):
    ENTRIES_TO_PROJECT_GT_STATS_MV = 'entries_to_project_gt_stats_mv'
    PROJECT_GT_STATS_TO_GT_STATS_MV = 'project_gt_stats_to_gt_stats_mv'


ClickHouseEntity = ClickHouseDictionary | ClickHouseTable | ClickHouseMaterializedView


@dataclass
class TableNameBuilder:
    reference_genome: ReferenceGenome
    dataset_type: DatasetType
    run_id: str

    @property
    def run_id_hash(self):
        sha256 = hashlib.sha256()
        sha256.update(self.run_id.encode())
        return sha256.hexdigest()[:8]

    def dst_table(self, clickhouse_entity: ClickHouseEntity):
        return f'{Env.CLICKHOUSE_DATABASE}.`{self.reference_genome.value}/{self.dataset_type.value}/{clickhouse_entity.value}`'

    def staging_dst_table(self, clickhouse_table: ClickHouseTable):
        return f'{STAGING_CLICKHOUSE_DATABASE}.`{self.run_id_hash}/{self.reference_genome.value}/{self.dataset_type.value}/{clickhouse_table.value}`'

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


def logged_query(query, params=None, increased_timeout: bool = False):
    client = get_clickhouse_client(increased_timeout)
    sanitized_query = query
    if Env.CLICKHOUSE_GCS_HMAC_KEY:
        sanitized_query = sanitized_query.replace(
            Env.CLICKHOUSE_GCS_HMAC_KEY,
            REDACTED,
        )
    if Env.CLICKHOUSE_GCS_HMAC_SECRET:
        sanitized_query = sanitized_query.replace(
            Env.CLICKHOUSE_GCS_HMAC_SECRET,
            REDACTED,
        )
    if Env.CLICKHOUSE_PASSWORD:
        sanitized_query = sanitized_query.replace(replace
            Env.CLICKHOUSE_PASSWORD,
            REDACTED,
        )
    logger.info(f'Executing query: {sanitized_query} | Params: {params}')
    return client.execute(query, params)


@retry()
def drop_staging_db():
    logged_query(f'DROP DATABASE IF EXISTS {STAGING_CLICKHOUSE_DATABASE};')


def dst_key_exists(
    table_name_builder: TableNameBuilder,
    clickhouse_table: ClickHouseTable,
    key: int | str,
) -> int:
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM {table_name_builder.dst_table(clickhouse_table)}
            WHERE {clickhouse_table.key_field} = %(key)s
        )
        """
    return logged_query(query, {'key': key})[0][0]


def max_src_key(
    table_name_builder: TableNameBuilder,
    clickhouse_table: ClickHouseTable,
) -> int:
    return logged_query(
        f"""
        SELECT max({clickhouse_table.key_field}) FROM {table_name_builder.src_table(clickhouse_table)}
        """,
    )[0][0]


def create_staging_entities(
    table_name_builder: TableNameBuilder,
) -> None:
    logged_query(
        f"""
        CREATE DATABASE {STAGING_CLICKHOUSE_DATABASE}
        """,
    )
    for clickhouse_table in [
        ClickHouseTable.ENTRIES,
        ClickHouseTable.PROJECT_GT_STATS,
    ]:
        logged_query(
            f"""
            CREATE
            TABLE {table_name_builder.staging_dst_table(clickhouse_table)}
            AS {table_name_builder.dst_table(clickhouse_table)}
            """,
        )

    # Special system logic for the materialized view & dictionary sourced from
    # the reference script: https://github.com/ClickHouse/examples/blob/cc4287fe759e67fd7af0ab3a5a79b42ac0c5a969/large_data_loads/src/worker.py#L523
    # CREATE MATERIALIZED VIEW AS does not work for the incremental view, and requires manipulating
    # the source view's create statement.
    for entity, replace_count in [
        # NOTE: the staging materialized view source is a staging table.
        # e.g CREATE t_mv TO t_a as SELECT * FROM t_b
        # However, the dictionary table source is a production table.
        (ClickHouseMaterializedView.ENTRIES_TO_PROJECT_GT_STATS_MV, 3),
        (ClickHouseDictionary.GT_STATS_DICT, 1),
    ]:
        create_view_statement = logged_query(
            """
            SELECT create_table_query FROM system.tables
            WHERE
            database = %(database)s
            AND name = %(name)s
            """,
            {
                'database': Env.CLICKHOUSE_DATABASE,
                'name': table_name_builder.dst_table(entity)
                .split('.')[1]
                .replace('`', ''),
            },
        )[0][0]
        logged_query(
            create_view_statement.replace(
                Env.CLICKHOUSE_DATABASE,
                STAGING_CLICKHOUSE_DATABASE,
                replace_count,
            )
            .replace(
                table_name_builder.reference_genome,
                f'{table_name_builder.run_id_hash}/{table_name_builder.reference_genome.value}',
                replace_count,
            )
            .replace("'[HIDDEN]'", Env.CLICKHOUSE_PASSWORD or "''"),
        )


def stage_existing_project_partitions(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
):
    for clickhouse_table in [
        ClickHouseTable.ENTRIES,
        ClickHouseTable.PROJECT_GT_STATS,
    ]:
        for project_guid in project_guids:
            # Note that ClickHouse successfully handles the case where the project
            # does not already exist in the dst table.  We simply attach an empty partition!
            logged_query(
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
    logged_query(
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
    logged_query(
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
        ClickHouseTable.PROJECT_GT_STATS,
    ]:
        logged_query(
            f"""
            OPTIMIZE TABLE {table_name_builder.staging_dst_table(clickhouse_table)} FINAL
            """,
            # For OPTIMIZE TABLE queries, server is known to not respond with output
            # or progress to the client.
            increased_timeout=True,
        )


def replace_project_partitions(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
) -> None:
    for clickhouse_table in [
        ClickHouseTable.ENTRIES,
        ClickHouseTable.PROJECT_GT_STATS,
    ]:
        for project_guid in project_guids:
            logged_query(
                f"""
                ALTER TABLE {table_name_builder.dst_table(clickhouse_table)}
                REPLACE PARTITION %(project_guid)s FROM {table_name_builder.staging_dst_table(clickhouse_table)}
                """,
                {'project_guid': project_guid},
            )


def refresh_materialized_view(
    table_name_builder: TableNameBuilder,
    clickhouse_materialized_view: ClickHouseMaterializedView,
) -> None:
    logged_query(
        f"""
        SYSTEM REFRESH VIEW {table_name_builder.dst_table(clickhouse_materialized_view)}
        """,
    )
    logged_query(
        # REFRESH VIEW returns immediately, requiring a WAIT.
        f"""
        SYSTEM WAIT VIEW {table_name_builder.dst_table(clickhouse_materialized_view)}
        """,
    )


def reload_staging_dictionary(
    table_name_builder: TableNameBuilder,
    clickhouse_dictionary: ClickHouseDictionary,
) -> None:
    logged_query(
        f"""
        SYSTEM RELOAD DICTIONARY {table_name_builder.staging_dst_table(clickhouse_dictionary)}
        """,
    )


def exchange_staging_production_dictionaries(
    table_name_builder: TableNameBuilder,
    clickhouse_dictionary: ClickHouseDictionary,
) -> None:
    logged_query(
        f"""
        EXCHANGE DICTIONARIES {table_name_builder.staging_dst_table(clickhouse_dictionary)} AND {table_name_builder.dst_table(clickhouse_dictionary)}
        """,
    )


@retry()
def direct_insert(
    clickhouse_table: ClickHouseTable,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    **_,
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
    logged_query(
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
    **_,
) -> None:
    table_name_builder = TableNameBuilder(
        reference_genome,
        dataset_type,
        run_id,
    )
    drop_staging_db()
    create_staging_entities(table_name_builder)
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
    refresh_materialized_view(
        table_name_builder,
        ClickHouseMaterializedView.PROJECT_GT_STATS_TO_GT_STATS_MV,
    )
    reload_staging_dictionary(
        table_name_builder,
        ClickHouseDictionary.GT_STATS_DICT,
    )
    exchange_staging_production_dictionaries(
        table_name_builder,
        ClickHouseDictionary.GT_STATS_DICT,
    )
    drop_staging_db()


def get_clickhouse_client(increased_timeout: bool = False) -> Client:
    return Client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        user=Env.CLICKHOUSE_USER,
        **{'password': Env.CLICKHOUSE_PASSWORD} if Env.CLICKHOUSE_PASSWORD else {},
        **{'send_receive_timeout': 3600} if increased_timeout else {},
    )
