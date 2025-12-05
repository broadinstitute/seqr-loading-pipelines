import ast
import functools
import hashlib
import math
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from string import Template

from clickhouse_driver import Client

from v03_pipeline.lib.core import DatasetType, ReferenceGenome
from v03_pipeline.lib.core.environment import Env
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
    new_transcripts_parquet_path,
    new_variants_parquet_path,
)

logger = get_logger(__name__)

GCS_NAMED_COLLECTION = 'pipeline_data_access'
GOOGLE_XML_API_PATH = 'https://storage.googleapis.com/'
OPTIMIZE_TABLE_TIMEOUT_S = 99999
WAIT_VIEW_TIMEOUT_S = 900
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
    def select_fields(self) -> str:
        return {
            ClickHouseTable.KEY_LOOKUP: 'variantId, key',
        }.get(self, '*')

    @property
    def insert(self) -> Callable:
        return {
            ClickHouseTable.ANNOTATIONS_MEMORY: direct_insert_annotations,
            ClickHouseTable.ENTRIES: atomic_insert_entries,
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
    def for_dataset_type(cls, dataset_type: DatasetType) -> list['ClickHouseTable']:
        tables = [
            ClickHouseTable.ANNOTATIONS_MEMORY,
            ClickHouseTable.KEY_LOOKUP,
        ]
        if dataset_type.should_write_new_transcripts:
            tables = [
                *tables,
                ClickHouseTable.TRANSCRIPTS,
            ]
        return [
            *tables,
            ClickHouseTable.ENTRIES,
        ]

    @classmethod
    def for_dataset_type_disk_backed_annotations_tables(
        cls,
        _dataset_type: DatasetType,
    ) -> list['ClickHouseTable']:
        return [
            ClickHouseTable.ANNOTATIONS_DISK,
        ]

    @classmethod
    def for_dataset_type_atomic_entries_update(
        cls,
        dataset_type: DatasetType,
    ) -> list['ClickHouseTable']:
        return [
            *cls.for_dataset_type_atomic_entries_update_project_partitioned(
                dataset_type,
            ),
            *cls.for_dataset_type_atomic_entries_update_unpartitioned(dataset_type),
        ]

    @classmethod
    def for_dataset_type_atomic_entries_update_project_partitioned(
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
    def for_dataset_type_atomic_entries_update_unpartitioned(
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
    ENTRIES_TO_PROJECT_GT_STATS_MV = 'entries_to_project_gt_stats_mv'
    PROJECT_GT_STATS_TO_GT_STATS_MV = 'project_gt_stats_to_gt_stats_mv'

    @classmethod
    def for_dataset_type_atomic_entries_update(
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
    def for_dataset_type_atomic_entries_update_refreshable(
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
            return f"gcs({GCS_NAMED_COLLECTION}, url='{path.replace('gs://', GOOGLE_XML_API_PATH)}')"
        return f"file('{path}', 'Parquet')"


class ClickhouseReferenceData(StrEnum):
    CLINVAR = 'clinvar'

    @classmethod
    def for_dataset_type(cls, dataset_type: DatasetType):
        if dataset_type in {DatasetType.SV, DatasetType.GCNV}:
            return []
        return [ClickhouseReferenceData.CLINVAR]

    def search_is_join_table(self):
        return self == ClickhouseReferenceData.CLINVAR

    def all_variants_path(self, table_name_builder: TableNameBuilder) -> str:
        return (
            f'{table_name_builder.dst_prefix}/reference_data/{self.value}/all_variants`'
        )

    def seqr_variants_path(self, table_name_builder: TableNameBuilder) -> str:
        return f'{table_name_builder.dst_prefix}/reference_data/{self.value}/seqr_variants`'

    def search_path(self, table_name_builder: TableNameBuilder) -> str:
        return f'{table_name_builder.dst_prefix}/reference_data/{self.value}`'

    def seqr_variants_to_search_mv_path(
        self,
        table_name_builder: TableNameBuilder,
    ) -> str:
        return f'{table_name_builder.dst_prefix}/reference_data/{self.value}/seqr_variants_to_search_mv`'

    def refresh(
        self,
        table_name_builder: TableNameBuilder,
    ):
        drop_staging_db()
        logged_query(
            f"""
            CREATE DATABASE {STAGING_CLICKHOUSE_DATABASE}
            """,
        )
        logged_query(
            f"""
            CREATE TABLE {table_name_builder.staging_dst_prefix}/_tmp_loadable_variantIds` ENGINE = Set AS (
                SELECT {ClickHouseTable.KEY_LOOKUP.key_field}
                FROM {table_name_builder.src_table(ClickHouseTable.KEY_LOOKUP)}
            )
            """,
        )
        logged_query(
            f"""
            INSERT INTO {self.seqr_variants_path(table_name_builder)}
            SELECT
                DISTINCT ON (key)
                dst.key,
                COLUMNS('.*') EXCEPT(version, variantId, key)
            FROM {self.all_variants_path(table_name_builder)} src
            INNER JOIN {table_name_builder.dst_table(ClickHouseTable.KEY_LOOKUP)} dst
            ON {ClickHouseTable.KEY_LOOKUP.join_condition}
            WHERE src.variantId IN {table_name_builder.staging_dst_prefix}/_tmp_loadable_variantIds`
            """,
        )
        if self.search_is_join_table:
            logged_query(
                f"""
                SYSTEM REFRESH VIEW {self.seqr_variants_to_search_mv_path(table_name_builder)}
                """,
            )
            logged_query(
                f"""
                SYSTEM WAIT VIEW {self.seqr_variants_to_search_mv_path(table_name_builder)}
                """,
                timeout=WAIT_VIEW_TIMEOUT_S,
            )
        else:
            logged_query(
                f"""
                SYSTEM RELOAD DICTIONARY {self.search_path(table_name_builder)}
                """,
            )


def logged_query(query, params=None, timeout: int | None = None):
    client = get_clickhouse_client(timeout)
    sanitized_query = query
    if Env.CLICKHOUSE_WRITER_PASSWORD:
        sanitized_query = sanitized_query.replace(
            Env.CLICKHOUSE_WRITER_PASSWORD,
            REDACTED,
        )
    logger.info(f'Executing query: {sanitized_query} | Params: {params}')
    return client.execute(query, params)


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


def get_create_mv_statements(
    table_name_builder: TableNameBuilder,
    clickhouse_mv: ClickHouseMaterializedView,
) -> tuple[str, str]:
    return logged_query(
        """
        SELECT create_table_query, as_select FROM system.tables
        WHERE
        engine = 'MaterializedView'
        AND database = %(database)s
        AND name = %(name)s
        """,
        {
            'database': Env.CLICKHOUSE_DATABASE,
            'name': table_name_builder.dst_table(clickhouse_mv)
            .split('.')[1]
            .replace('`', ''),
        },
    )[0]


def normalize_partition(partition: str) -> tuple:
    """
    Ensure a ClickHouse partition expression is always returned as a tuple.
    'project_d'       -> ('project_d',)
    "('project_d', 0)" -> ('project_d', 0)
    """
    if not isinstance(partition, str):
        msg = f'Unsupported partition type: {type(partition)}'
        raise TypeError(msg)
    partition = partition.strip()
    if partition.startswith('(') and partition.endswith(')'):
        return ast.literal_eval(partition)
    return (partition,)


def get_partitions_for_projects(
    table_name_builder: TableNameBuilder,
    clickhouse_table: ClickHouseTable,
    project_guids: list[str],
    staging=False,
):
    rows = logged_query(
        """
        SELECT DISTINCT partition
        FROM system.parts
        WHERE
            database = %(database)s
            AND table = %(table)s
            AND multiSearchAny(partition, %(project_guids)s)
        """,
        {
            'database': STAGING_CLICKHOUSE_DATABASE
            if staging
            else Env.CLICKHOUSE_DATABASE,
            'table': (
                table_name_builder.staging_dst_table(clickhouse_table)
                if staging
                else table_name_builder.dst_table(clickhouse_table)
            )
            .split('.')[1]
            .replace('`', ''),
            'project_guids': project_guids,
        },
    )
    return [normalize_partition(row[0]) for row in rows]


def create_staging_materialized_views(
    table_name_builder: TableNameBuilder,
    clickhouse_mvs: list[ClickHouseMaterializedView],
):
    for clickhouse_mv in clickhouse_mvs:
        create_table_statement = get_create_mv_statements(
            table_name_builder,
            clickhouse_mv,
        )[0]
        create_table_statement = create_table_statement.replace(
            table_name_builder.dst_prefix,
            table_name_builder.staging_dst_prefix,
        )
        logged_query(create_table_statement)


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
        for partition in get_partitions_for_projects(
            table_name_builder,
            clickhouse_table,
            project_guids,
        ):
            # Note that ClickHouse successfully handles the case where the project
            # does not already exist in the dst table.  We simply attach an empty partition!
            logged_query(
                f"""
                ALTER TABLE {table_name_builder.staging_dst_table(clickhouse_table)}
                ATTACH PARTITION %(partition)s FROM {table_name_builder.dst_table(clickhouse_table)}
                """,
                {'partition': partition},
            )


def delete_existing_families_from_staging_entries(
    table_name_builder: TableNameBuilder,
    family_guids: list[str],
) -> None:
    logged_query(
        f"""
        INSERT INTO {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)}
        SELECT COLUMNS('.*') EXCEPT(sign, n_partitions, partition_id), -1 as sign
        FROM {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)}
        WHERE family_guid in %(family_guids)s
        """,
        {'family_guids': family_guids},
    )


def insert_new_entries(
    table_name_builder: TableNameBuilder,
) -> None:
    dst_cols = [
        r[0]
        for r in logged_query(
            f'DESCRIBE TABLE {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)}',
        )
    ]
    src_cols = [
        r[0]
        for r in logged_query(
            f'DESCRIBE TABLE {table_name_builder.src_table(ClickHouseTable.ENTRIES)}',
        )
    ]
    common, overrides = [c for c in dst_cols if c in src_cols], {}
    if 'geneId_ids' in dst_cols and 'geneIds' in src_cols:
        common = [c for c in common if c not in ('geneId_ids', 'geneIds')]
        common.append('geneId_ids')
        overrides = {
            'geneId_ids': (
                f"""
                arrayFilter(
                    x -> x IS NOT NULL,
                    arrayMap(
                        g -> dictGetOrNull(
                            {Env.CLICKHOUSE_DATABASE}.`seqrdb_gene_ids`,
                            'seqrdb_id',
                            g
                        ),
                        geneIds
                    )
                )
                """
            ),
        }
    dst_list = ', '.join(common)
    src_list = ', '.join([overrides.get(c, c) for c in common])
    logged_query(
        f"""
        INSERT INTO {table_name_builder.staging_dst_table(ClickHouseTable.ENTRIES)} ({dst_list})
        SELECT {src_list}
        FROM {table_name_builder.src_table(ClickHouseTable.ENTRIES)}
        """,
    )


@retry(tries=2)
def optimize_entries(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
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
        if decrs_exist:
            if merges_running:
                logger.info('Decrs exist and merges are running, so waiting')
            else:
                logger.info('Decrs exist and no merges are running, so optimizing')
                partitions = get_partitions_for_projects(
                    table_name_builder,
                    ClickHouseTable.ENTRIES,
                    project_guids,
                    staging=True,
                )
                table_name = table_name_builder.staging_dst_table(
                    ClickHouseTable.ENTRIES,
                )
                optimize_statements = [
                    f'OPTIMIZE TABLE {table_name} PARTITION {partition} FINAL'
                    for partition in partitions
                ]
                parallel_optimize_sql = '\nPARALLEL WITH\n'.join(optimize_statements)
                logged_query(
                    parallel_optimize_sql,
                    timeout=OPTIMIZE_TABLE_TIMEOUT_S,
                )
            time.sleep(Env.CLICKHOUSE_OPTIMIZE_TABLE_WAIT_S)
        else:
            safely_optimized = True


@retry(tries=2)
def refresh_materialized_views(
    table_name_builder,
    materialized_views: list[ClickHouseMaterializedView],
    staging=False,
):
    for materialized_view in materialized_views:
        logged_query(
            f"""
            SYSTEM REFRESH VIEW {table_name_builder.staging_dst_table(materialized_view) if staging else table_name_builder.dst_table(materialized_view)}
            """,
        )
        logged_query(
            f"""
            SYSTEM WAIT VIEW {table_name_builder.staging_dst_table(materialized_view) if staging else table_name_builder.dst_table(materialized_view)}
            """,
            timeout=WAIT_VIEW_TIMEOUT_S,
        )


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


@retry(tries=2)
def reload_dictionaries(
    table_name_builder: TableNameBuilder,
    dictionaries: list[ClickHouseDictionary],
):
    for dictionary in dictionaries:
        logged_query(
            f"""
            SYSTEM RELOAD DICTIONARY {table_name_builder.dst_table(dictionary)}
            """,
        )


def replace_project_partitions(
    table_name_builder: TableNameBuilder,
    clickhouse_tables: list[ClickHouseTable],
    project_guids: list[str],
) -> None:
    for clickhouse_table in clickhouse_tables:
        for partition in get_partitions_for_projects(
            table_name_builder,
            clickhouse_table,
            project_guids,
            staging=True,
        ):
            logged_query(
                f"""
                ALTER TABLE {table_name_builder.dst_table(clickhouse_table)}
                REPLACE PARTITION %(partition)s FROM {table_name_builder.staging_dst_table(clickhouse_table)}
                """,
                {'partition': partition},
            )


# Note this is NOT idempotent, as running the swap twice will
# result in the tables not being swapped.
def exchange_tables(
    table_name_builder,
    clickhouse_tables: list[ClickHouseTable],
) -> None:
    for clickhouse_table in clickhouse_tables:
        logged_query(
            f"""
            EXCHANGE TABLES {table_name_builder.staging_dst_table(clickhouse_table)} AND {table_name_builder.dst_table(clickhouse_table)}
            """,
        )


def direct_insert_annotations(
    table_name_builder: TableNameBuilder,
    **_,
) -> None:
    dst_table = table_name_builder.dst_table(ClickHouseTable.ANNOTATIONS_MEMORY)
    src_table = table_name_builder.src_table(ClickHouseTable.ANNOTATIONS_MEMORY)
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
            SELECT {ClickHouseTable.ANNOTATIONS_MEMORY.key_field}
            FROM {src_table} src
            LEFT ANTI JOIN {dst_table} dst
            ON {ClickHouseTable.ANNOTATIONS_MEMORY.join_condition}
        )
        """,
    )
    for (
        clickhouse_table
    ) in ClickHouseTable.for_dataset_type_disk_backed_annotations_tables(
        table_name_builder.dataset_type,
    ):
        disk_backed_dst_table = table_name_builder.dst_table(clickhouse_table)
        disk_backed_src_table = table_name_builder.src_table(clickhouse_table)
        logged_query(
            f"""
            INSERT INTO {disk_backed_dst_table}
            SELECT {clickhouse_table.select_fields}
            FROM {disk_backed_src_table} WHERE {clickhouse_table.key_field} IN {table_name_builder.staging_dst_prefix}/_tmp_loadable_keys`
            """,
        )
    logged_query(
        f"""
        INSERT INTO {dst_table}
        SELECT {ClickHouseTable.ANNOTATIONS_MEMORY.select_fields}
        FROM {src_table} WHERE {ClickHouseTable.ANNOTATIONS_MEMORY.key_field} IN {table_name_builder.staging_dst_prefix}/_tmp_loadable_keys`
        """,
    )
    drop_staging_db()


def direct_insert_all_keys(
    clickhouse_table: ClickHouseTable,
    table_name_builder: TableNameBuilder,
    **_,
) -> None:
    dst_table = table_name_builder.dst_table(clickhouse_table)
    src_table = table_name_builder.src_table(clickhouse_table)
    logged_query(
        f"""
        INSERT INTO {dst_table}
        SELECT {clickhouse_table.select_fields}
        FROM {src_table}
        """,
    )


# This is a smattering of shared operations that lacks a better name :/
def finalize_refresh_flow(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
):
    dataset_type = table_name_builder.dataset_type
    refresh_materialized_views(
        table_name_builder,
        ClickHouseMaterializedView.for_dataset_type_atomic_entries_update_refreshable(
            dataset_type,
        ),
        staging=True,
    )
    replace_project_partitions(
        table_name_builder,
        ClickHouseTable.for_dataset_type_atomic_entries_update_project_partitioned(
            dataset_type,
        ),
        project_guids,
    )
    exchange_tables(
        table_name_builder,
        ClickHouseTable.for_dataset_type_atomic_entries_update_unpartitioned(
            dataset_type,
        ),
    )
    drop_staging_db()
    reload_dictionaries(
        table_name_builder,
        ClickHouseDictionary.for_dataset_type(dataset_type),
    )


def atomic_insert_entries(
    table_name_builder: TableNameBuilder,
    project_guids: list[str],
    family_guids: list[str],
    **_,
) -> None:
    dataset_type = table_name_builder.dataset_type
    drop_staging_db()
    create_staging_tables(
        table_name_builder,
        ClickHouseTable.for_dataset_type_atomic_entries_update(dataset_type),
    )
    create_staging_materialized_views(
        table_name_builder,
        ClickHouseMaterializedView.for_dataset_type_atomic_entries_update(
            dataset_type,
        ),
    )
    stage_existing_project_partitions(
        table_name_builder,
        project_guids,
        ClickHouseTable.for_dataset_type_atomic_entries_update_project_partitioned(
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
        project_guids,
    )
    validate_family_guid_counts(
        table_name_builder,
        project_guids,
        family_guids,
    )
    finalize_refresh_flow(table_name_builder, project_guids)


@retry()
def load_complete_run(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    project_guids: list[str],
    family_guids: list[str],
):
    msg = f'Attempting load of run: {reference_genome.value}/{dataset_type.value}/{run_id}'
    logger.info(msg)
    table_name_builder = TableNameBuilder(
        reference_genome,
        dataset_type,
        run_id,
    )
    for clickhouse_table in ClickHouseTable.for_dataset_type(dataset_type):
        clickhouse_table.insert(
            table_name_builder=table_name_builder,
            project_guids=project_guids,
            family_guids=family_guids,
        )
    for clickhouse_reference_data in ClickhouseReferenceData.for_dataset_type(
        dataset_type,
    ):
        clickhouse_reference_data.refresh(
            table_name_builder=table_name_builder,
        )


@retry()
def delete_family_guids(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    project_guid: str,
    family_guids: list[str],
):
    msg = f'Attempting delete families for {reference_genome.value}/{dataset_type.value} {project_guid}: {family_guids}'
    logger.info(msg)
    table_name_builder = TableNameBuilder(
        reference_genome,
        dataset_type,
        run_id,
    )
    entries_exist = logged_query(
        f"""
        SELECT EXISTS (
            SELECT 1
            FROM {table_name_builder.dst_table(ClickHouseTable.ENTRIES)}
            WHERE project_guid = %(project_guid)s
            AND has(%(family_guids)s, family_guid)
        );
        """,
        {'family_guids': family_guids, 'project_guid': project_guid},
    )[0][0]
    if not entries_exist:
        msg = f'No data exists for {reference_genome.value} & {dataset_type.value} so skipping'
        logger.info(msg)
        return
    project_guids = [project_guid]
    drop_staging_db()
    create_staging_tables(
        table_name_builder,
        ClickHouseTable.for_dataset_type_atomic_entries_update(dataset_type),
    )
    create_staging_materialized_views(
        table_name_builder,
        ClickHouseMaterializedView.for_dataset_type_atomic_entries_update(
            dataset_type,
        ),
    )
    stage_existing_project_partitions(
        table_name_builder,
        project_guids,
        ClickHouseTable.for_dataset_type_atomic_entries_update_project_partitioned(
            dataset_type,
        ),
    )
    delete_existing_families_from_staging_entries(
        table_name_builder,
        family_guids,
    )
    optimize_entries(
        table_name_builder,
        project_guids,
    )
    finalize_refresh_flow(table_name_builder, project_guids)


@retry()
def rebuild_gt_stats(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    project_guids: list[str],
) -> None:
    if ClickHouseDictionary.GT_STATS_DICT not in ClickHouseDictionary.for_dataset_type(
        dataset_type,
    ):
        msg = f'Skipping gt stats rebuild for {reference_genome.value}/{dataset_type.value} {project_guids[:10]}...'
        logger.info(msg)
        return
    msg = f'Attempting rebuild gt stats for {reference_genome.value}/{dataset_type.value} {project_guids[:10]}...'
    logger.info(msg)
    table_name_builder = TableNameBuilder(
        reference_genome,
        dataset_type,
        run_id,
    )
    drop_staging_db()
    create_staging_tables(
        table_name_builder,
        ClickHouseTable.for_dataset_type_atomic_entries_update(dataset_type),
    )
    create_staging_materialized_views(
        table_name_builder,
        ClickHouseMaterializedView.for_dataset_type_atomic_entries_update(
            dataset_type,
        ),
    )
    stage_existing_project_partitions(
        table_name_builder,
        project_guids,
        ClickHouseTable.for_dataset_type_atomic_entries_update_project_partitioned(
            dataset_type,
        ),
    )
    for partition in get_partitions_for_projects(
        table_name_builder,
        ClickHouseTable.PROJECT_GT_STATS,
        project_guids,
        staging=True,
    ):
        logged_query(
            f"""
            ALTER TABLE {table_name_builder.staging_dst_table(ClickHouseTable.PROJECT_GT_STATS)}
            DROP PARTITION %(partition)s
            """,
            {'partition': partition},
        )
    select_statement = get_create_mv_statements(
        table_name_builder,
        ClickHouseMaterializedView.ENTRIES_TO_PROJECT_GT_STATS_MV,
    )[1]
    select_statement = select_statement.replace(
        table_name_builder.dst_prefix,
        table_name_builder.staging_dst_prefix,
    )
    # NB: encountered OOMs with large projects, necessitating sharding the insertion query.
    max_key = logged_query(
        f"""
        SELECT max(key) FROM {table_name_builder.dst_table(ClickHouseTable.GT_STATS)}
        """,
    )[0][0]
    step = math.ceil(max_key / 5)
    for range_start in range(0, max_key, step):
        range_end = min(range_start + step, max_key + 1)
        logged_query(
            f"""
            INSERT INTO {
                table_name_builder.staging_dst_table(ClickHouseTable.PROJECT_GT_STATS)
            }
            {
                select_statement.replace(
                    'GROUP BY project_guid',
                    'WHERE key >= %(range_start)s AND key < %(range_end)s GROUP BY project_guid',
                )
            }
            """,
            {'range_start': range_start, 'range_end': range_end},
        )
    finalize_refresh_flow(table_name_builder, project_guids)


def get_clickhouse_client(
    timeout: int | None = None,
    database: str | None = None,
) -> Client:
    return Client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        user=Env.CLICKHOUSE_WRITER_USER,
        password=Env.CLICKHOUSE_WRITER_PASSWORD,
        **{'database': database} if database else {},
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
