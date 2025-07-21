import functools
import hashlib
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from string import Template

from clickhouse_driver import Client

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
    new_transcripts_parquet_path,
    new_variants_parquet_path,
)

logger = get_logger(__name__)

GOOGLE_XML_API_PATH = 'https://storage.googleapis.com/'
OPTIMIZE_TABLE_WAIT_S = 300
OPTIMIZE_TABLE_TIMEOUT_S = 99999
REDACTED = 'REDACTED'
STAGING_CLICKHOUSE_DATABASE = 'staging'


class ClickHouseTable(StrEnum):
    ANNOTATIONS_DISK = 'annotations_disk'
    ANNOTATIONS_MEMORY = 'annotations_memory'
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
            ClickHouseTable.KEY_LOOKUP: new_variants_parquet_path,
            ClickHouseTable.TRANSCRIPTS: new_transcripts_parquet_path,
            ClickHouseTable.ENTRIES: new_entries_parquet_path,
        }[self]

    def should_load(
        self,
        dataset_type: DatasetType,
    ):
        if self == ClickHouseTable.TRANSCRIPTS:
            return dataset_type.should_write_new_transcripts
        return self in {
            ClickHouseTable.ANNOTATIONS_DISK,
            ClickHouseTable.ANNOTATIONS_MEMORY,
            ClickHouseTable.KEY_LOOKUP,
            ClickHouseTable.ENTRIES,
        }

    @property
    def key_field(self):
        return 'variantId' if self == ClickHouseTable.KEY_LOOKUP else 'key'

    @property
    def join_condition(self):
        return (
            'assumeNotNull(src.variantId) = dst.variantId'
            if self == ClickHouseTable.KEY_LOOKUP
            else 'assumeNotNull(toUInt32(src.key)) = dst.key'
        )

    @property
    def select_fields(self):
        return 'variantId, key' if self == ClickHouseTable.KEY_LOOKUP else '*'

    @property
    def insert(self) -> Callable:
        return {
            ClickHouseTable.ANNOTATIONS_DISK: functools.partial(
                direct_insert_new_keys,
                clickhouse_table=self,
            ),
            ClickHouseTable.ANNOTATIONS_MEMORY: functools.partial(
                direct_insert_new_keys,
                clickhouse_table=self,
            ),
            ClickHouseTable.ENTRIES: functools.partial(
                atomic_entries_insert,
                _clickhouse_table=self,
            ),
            ClickHouseTable.KEY_LOOKUP: functools.partial(
                direct_insert_all_keys,
                clickhouse_table=self,
            ),
            ClickHouseTable.TRANSCRIPTS: functools.partial(
                direct_insert_all_keys,
                clickhouse_table=self,
            ),
        }[self]

    @classmethod
    def for_dataset_type_atomic_entries_insert(
        cls,
        dataset_type: DatasetType,
    ) -> list['ClickHouseTable']:
        return [
            *cls.for_dataset_type_atomic_entries_insert_project_partitioned(
                dataset_type,
            ),
            *cls.for_dataset_type_atomic_entries_insert_unpartitioned(dataset_type),
        ]

    @classmethod
    def for_dataset_type_atomic_entries_insert_project_partitioned(
        cls,
        dataset_type: DatasetType,
    ) -> list['ClickHouseTable']:
        if dataset_type == DatasetType.GCNV:
            return [ClickHouseTable.ENTRIES]
        return [
            ClickHouseTable.ENTRIES,
            ClickHouseTable.PROJECT_GT_STATS,
        ]

    @classmethod
    def for_dataset_type_atomic_entries_insert_unpartitioned(
        cls,
        dataset_type: DatasetType,
    ) -> list['ClickHouseTable']:
        if dataset_type == DatasetType.GCNV:
            return []
        return [ClickHouseTable.GT_STATS]


class ClickHouseDictionary(StrEnum):
    GT_STATS_DICT = 'gt_stats_dict'

    @classmethod
    def for_dataset_type(
        cls,
        dataset_type: DatasetType,
    ) -> list['ClickHouseDictionary']:
        if dataset_type == DatasetType.GCNV:
            return []
        return list(cls)


class ClickHouseMaterializedView(StrEnum):
    CLINVAR_ALL_VARIANTS_TO_CLINVAR_MV = 'clinvar_all_variants_to_clinvar_mv'
    ENTRIES_TO_PROJECT_GT_STATS_MV = 'entries_to_project_gt_stats_mv'
    PROJECT_GT_STATS_TO_GT_STATS_MV = 'project_gt_stats_to_gt_stats_mv'

    @classmethod
    def for_dataset_type_atomic_entries_insert(
        cls,
        dataset_type: DatasetType,
    ) -> list['ClickHouseMaterializedView']:
        if dataset_type == DatasetType.GCNV:
            return []
        return [
            ClickHouseMaterializedView.ENTRIES_TO_PROJECT_GT_STATS_MV,
            ClickHouseMaterializedView.PROJECT_GT_STATS_TO_GT_STATS_MV,
        ]

    @classmethod
    def for_dataset_type_atomic_entries_insert_refreshable(
        cls,
        dataset_type: DatasetType,
    ) -> list['ClickHouseMaterializedView']:
        if dataset_type == DatasetType.GCNV:
            return []
        return [ClickHouseMaterializedView.PROJECT_GT_STATS_TO_GT_STATS_MV]


ClickHouseEntity = ClickHouseDictionary | ClickHouseTable | ClickHouseMaterializedView


@dataclass
class TableNameBuilder:
    reference_genome: ReferenceGenome
    dataset_type: DatasetType
    run_id: str

    @property
    def run_id_hash(self):
        # Note: encountered length issues with the default
        # run ids generated by the pipeline.  ClickHouse performed
        # well with staging Tables with the long run ids, but failed
        # to recognized staging Dictionaries.
        sha256 = hashlib.sha256()
        sha256.update(self.run_id.encode())
        return sha256.hexdigest()[:8]

    @property
    def dst_prefix(self):
        return f'{Env.CLICKHOUSE_DATABASE}.`{self.reference_genome.value}/{self.dataset_type.value}'

    def dst_table(self, clickhouse_entity: ClickHouseEntity):
        return f'{self.dst_prefix}/{clickhouse_entity.value}`'

    @property
    def staging_dst_prefix(self):
        return f'{STAGING_CLICKHOUSE_DATABASE}.`{self.run_id_hash}/{self.reference_genome.value}/{self.dataset_type.value}'

    def staging_dst_table(self, clickhouse_table: ClickHouseTable):
        return f'{self.staging_dst_prefix}/{clickhouse_table.value}`'

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


def logged_query(query, params=None, timeout: int | None = None):
    client = get_clickhouse_client(timeout)
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
        sanitized_query = sanitized_query.replace(
            Env.CLICKHOUSE_PASSWORD,
            REDACTED,
        )
    logger.info(f'Executing query: {sanitized_query} | Params: {params}')
    return client.execute(query, params)


@retry(delay=1)
def drop_staging_db():
    logged_query(f'DROP DATABASE IF EXISTS {STAGING_CLICKHOUSE_DATABASE};')


def create_staging_tables(
    table_name_builder: TableNameBuilder,
    clickhouse_tables: list[ClickHouseTable],
) -> None:
    logged_query(
        f"""
        CREATE DATABASE {STAGING_CLICKHOUSE_DATABASE}
        """,
    )
    for clickhouse_table in clickhouse_tables:
        logged_query(
            f"""
            CREATE
            TABLE {table_name_builder.staging_dst_table(clickhouse_table)}
            AS {table_name_builder.dst_table(clickhouse_table)}
            """,
        )


def create_staging_non_table_entities(
    table_name_builder: TableNameBuilder,
    clickhouse_entities: list[ClickHouseEntity],
):
    for clickhouse_entity in clickhouse_entities:
        create_entity_statement = logged_query(
            """
            SELECT create_table_query FROM system.tables
            WHERE
            database = %(database)s
            AND name = %(name)s
            """,
            {
                'database': Env.CLICKHOUSE_DATABASE,
                'name': table_name_builder.dst_table(clickhouse_entity)
                .split('.')[1]
                .replace('`', ''),
            },
        )[0][0]
        if isinstance(clickhouse_entity, ClickHouseDictionary):
            password = Env.CLICKHOUSE_PASSWORD or "''"
            create_entity_statement = create_entity_statement.replace(
                "PASSWORD '[HIDDEN]'",
                f'PASSWORD {password}',
            )
        create_entity_statement = create_entity_statement.replace(
            table_name_builder.dst_prefix,
            table_name_builder.staging_dst_prefix,
        )
        logged_query(create_entity_statement)


# Note that this function is NOT idemptotent.  Clickhouse permits
# attaching the same partition to a table multiple times.
def stage_existing_project_partitions(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
    clickhouse_tables: list[ClickHouseTable],
):
    for clickhouse_table in clickhouse_tables:
        # Very important piece here:
        # ALL projects in the project_gt_stats table are staged, allowing us to rebuild
        # a production-quality gt_stats materialized view in the staging environment.
        if clickhouse_table == ClickHouseTable.PROJECT_GT_STATS:
            logged_query(
                f"""
                ALTER TABLE {table_name_builder.staging_dst_table(clickhouse_table)}
                ATTACH PARTITION ALL FROM {table_name_builder.dst_table(clickhouse_table)}
                """,
            )
            continue
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


def delete_existing_families_from_staging_entries(
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


@retry(tries=5)
def optimize_entries(
    table_name_builder: TableNameBuilder,
) -> None:
    safely_optimized = False
    while not safely_optimized:
        decrs_exist = logged_query(
            f"""
            SELECT EXISTS (
                SELECT 1
                FROM {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)}
                WHERE sign = -1
            );
            """,
        )[0][0]
        merges_running = logged_query(
            """
            SELECT EXISTS (
                SELECT 1
                FROM system.merges
                WHERE database = %(database)s
                AND table = %(table)s
            );
            """,
            {
                'database': STAGING_CLICKHOUSE_DATABASE,
                'table': table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)
                .split('.')[1]
                .replace('`', ''),
            },
        )[0][0]
        if decrs_exist and merges_running:
            logger.info('Decrs exist and merges are running, so waiting')
            time.sleep(OPTIMIZE_TABLE_WAIT_S)
        elif not decrs_exist and merges_running:
            logger.info('No decrs exist but merges are running, so waiting')
            time.sleep(OPTIMIZE_TABLE_WAIT_S)
        elif decrs_exist and not merges_running:
            logged_query(
                f"""
                OPTIMIZE TABLE {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)} FINAL
                """,
                timeout=OPTIMIZE_TABLE_TIMEOUT_S,
            )
        else:
            safely_optimized = True


@retry(delay=5)
def refresh_materialized_views(
    table_name_builder,
    materialized_views: list[ClickHouseMaterializedView],
):
    for materialized_view in materialized_views:
        logged_query(
            f"""
            SYSTEM REFRESH VIEW {table_name_builder.staging_dst_table(materialized_view)}
            """,
        )
        logged_query(
            f"""
            SYSTEM WAIT VIEW {table_name_builder.staging_dst_table(materialized_view)}
            """,
            timeout=600,
        )


@retry(delay=5)
def validate_family_guid_counts(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
    family_guids: list[str],
) -> None:
    query = Template(
        """
        SELECT family_guid, COUNT(*)
        FROM $table_name
        WHERE project_guid in %(project_guids)s
        AND family_guid in %(family_guids)s
        GROUP BY 1
        """,
    )
    src_family_counts = dict(
        logged_query(
            query.substitute(
                table_name=table_name_builder.src_table(
                    ClickHouseTable.ENTRIES,
                ),
            ),
            {'family_guids': family_guids, 'project_guids': project_guids},
        ),
    )
    dst_family_counts = dict(
        logged_query(
            query.substitute(
                table_name=table_name_builder.staging_dst_table(
                    ClickHouseTable.ENTRIES,
                ),
            ),
            {'family_guids': family_guids, 'project_guids': project_guids},
        ),
    )
    if src_family_counts != dst_family_counts:
        msg = 'Loaded Row counts are different than expected.'
        raise ValueError(msg)


@retry(delay=5)
def reload_dictionaries(
    table_name_builder: TableNameBuilder,
    dictionaries: list[ClickHouseDictionary],
):
    for dictionary in dictionaries:
        logged_query(
            f"""
            SYSTEM RELOAD DICTIONARY {table_name_builder.staging_dst_table(dictionary)}
            """,
        )


@retry(delay=5)  # REPLACE partition is a copy, so this is idempotent.
def replace_project_partitions(
    table_name_builder: TableNameBuilder,
    clickhouse_tables: list[ClickHouseTable],
    project_guids: list[str],
) -> None:
    for clickhouse_table in clickhouse_tables:
        for project_guid in project_guids:
            logged_query(
                f"""
                ALTER TABLE {table_name_builder.dst_table(clickhouse_table)}
                REPLACE PARTITION %(project_guid)s FROM {table_name_builder.staging_dst_table(clickhouse_table)}
                """,
                {'project_guid': project_guid},
            )


# Note this is NOT idempotent, as running the swap twice will
# result in the entities not being swapped.
def exchange_entities(
    table_name_builder,
    clickhouse_entities: list[ClickHouseEntity],
) -> None:
    for clickhouse_entity in clickhouse_entities:
        logged_query(
            f"""
            EXCHANGE {'DICTIONARIES' if isinstance(clickhouse_entity, ClickHouseDictionary) else 'TABLES'} {table_name_builder.staging_dst_table(clickhouse_entity)} AND {table_name_builder.dst_table(clickhouse_entity)}
            """,
        )


@retry()
def direct_insert_new_keys(
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
    dst_table = table_name_builder.dst_table(clickhouse_table)
    src_table = table_name_builder.src_table(clickhouse_table)
    drop_staging_db()
    logged_query(
        f"""
        CREATE DATABASE {STAGING_CLICKHOUSE_DATABASE}
        """,
    )
    # NB: Unfortunately there's a bug(?) or inaccuracy if this is attempted without an intermediate
    # temporary table, likely due to writing to a table and joining against it at the same time.
    logged_query(
        f"""
        CREATE TABLE {table_name_builder.staging_dst_prefix}/_tmp_loadable_keys` ENGINE = Set AS (
            SELECT {clickhouse_table.key_field}
            FROM {src_table} src
            LEFT ANTI JOIN {dst_table} dst
            ON {clickhouse_table.join_condition}
        )
        """,
    )
    logged_query(
        f"""
        INSERT INTO {dst_table}
        SELECT {clickhouse_table.select_fields}
        FROM {src_table} WHERE {clickhouse_table.key_field} IN {table_name_builder.staging_dst_prefix}/_tmp_loadable_keys`
        """,
    )
    drop_staging_db()


@retry()
def direct_insert_all_keys(
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
    dst_table = table_name_builder.dst_table(clickhouse_table)
    src_table = table_name_builder.src_table(clickhouse_table)
    logged_query(
        f"""
        INSERT INTO {dst_table}
        SELECT {clickhouse_table.select_fields}
        FROM {src_table}
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
    create_staging_tables(
        table_name_builder,
        ClickHouseTable.for_dataset_type_atomic_entries_insert(dataset_type),
    )
    create_staging_non_table_entities(
        table_name_builder,
        [
            *ClickHouseMaterializedView.for_dataset_type_atomic_entries_insert(
                dataset_type,
            ),
            *ClickHouseDictionary.for_dataset_type(dataset_type),
        ],
    )
    stage_existing_project_partitions(
        table_name_builder,
        project_guids,
        ClickHouseTable.for_dataset_type_atomic_entries_insert_project_partitioned(
            dataset_type,
        ),
    )
    delete_existing_families_from_staging_entries(
        table_name_builder,
        family_guids,
    )
    insert_new_entries(
        table_name_builder,
    )
    optimize_entries(
        table_name_builder,
    )
    validate_family_guid_counts(
        table_name_builder,
        project_guids,
        family_guids,
    )
    refresh_materialized_views(
        table_name_builder,
        ClickHouseMaterializedView.for_dataset_type_atomic_entries_insert_refreshable(
            dataset_type,
        ),
    )
    replace_project_partitions(
        table_name_builder,
        ClickHouseTable.for_dataset_type_atomic_entries_insert_project_partitioned(
            dataset_type,
        ),
        project_guids,
    )
    exchange_entities(
        table_name_builder,
        ClickHouseTable.for_dataset_type_atomic_entries_insert_unpartitioned(
            dataset_type,
        ),
    )
    # Very important nuance here... the staged dictionary
    # source tables are production tables, so the
    # dictionary reload must happen 'after' the preceeding
    # exchange entity statement.  I (bpb) made several
    # attempts to have a staging dictionary source
    # a staging gt_stats table, but ran into issues with
    # the dictionary "EXCHANGE" leaving the query source
    # unmodified.  We ended up with a production dictionary
    # pointing at a staging source and a staging dictionary
    # pointing at a production source... which is not desired
    # behavior.
    reload_dictionaries(
        table_name_builder,
        ClickHouseDictionary.for_dataset_type(dataset_type),
    )
    exchange_entities(
        table_name_builder,
        ClickHouseDictionary.for_dataset_type(dataset_type),
    )
    drop_staging_db()


def get_clickhouse_client(
    timeout: int | None = None,
) -> Client:
    return Client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        user=Env.CLICKHOUSE_USER,
        **{'password': Env.CLICKHOUSE_PASSWORD} if Env.CLICKHOUSE_PASSWORD else {},
        **{'send_receive_timeout': timeout} if timeout else {},
        **{
            'settings': {
                'send_timeout': timeout,
                'receive_timeout': timeout,
            },
        }
        if timeout
        else {},
    )
