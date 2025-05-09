import hashlib
import json
import os
from collections.abc import Callable
from enum import StrEnum

import hailtop.fs as hfs
from clickhouse_driver import Client

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    metadata_for_run_path,
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
        if self == ClickHouseTable.GT_STATS:
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


@retry()
def drop_staging_db():
    logger.info('Dropping all staging tables')
    client = get_clickhouse_client()
    client.command(f'DROP DATABASE IF EXISTS {STAGING_CLICKHOUSE_DATABASE};')


def dst_key_exists(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    clickhouse_table: ClickHouseTable,
    key: int | str,
) -> int:
    client = get_clickhouse_client()
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM {Env.CLICKHOUSE_DATABASE}.`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}`
            WHERE {clickhouse_table.key_field} = %(key)s
        )
        """
    return client.execute(query, {'key': key})[0][0]


def max_src_key(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable,
) -> int:
    client = get_clickhouse_client()
    path = clickhouse_insert_table_fn(
        clickhouse_table.src_path_fn(reference_genome, dataset_type, run_id),
    )
    return client.execute(
        f"""
        SELECT max({clickhouse_table.key_field}) FROM {path}
        """,
    )[0][0]


def create_staging_entries(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable,
) -> None:
    client = get_clickhouse_client()
    run_id_hash = hashlib.sha256(
        run_id.encode('utf8'),
    ).hexdigest()
    client.execute(
        f"""
        CREATE
        TABLE {STAGING_CLICKHOUSE_DATABASE}.`{run_id_hash}`/`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}`
        AS {Env.CLICKHOUSE_DATABASE}.`{run_id_hash}`/`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}`
        """,
    )
    client.execute(
        f"""
        CREATE
        TABLE {STAGING_CLICKHOUSE_DATABASE}.`{run_id_hash}`/`{reference_genome.value}/{dataset_type.value}/{ClickHouseTable.GT_STATS.value}`
        AS {Env.CLICKHOUSE_DATABASE}.`{run_id_hash}`/`{reference_genome.value}/{dataset_type.value}/{ClickHouseTable.GT_STATS.value}`
        """,
    )
    client.execute(
        f"""
        CREATE
        VIEW {STAGING_CLICKHOUSE_DATABASE}.`{run_id_hash}`/`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}_to_{ClickHouseTable.GT_STATS.value}`
        AS {Env.CLICKHOUSE_DATABASE}.`{run_id_hash}`/`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}_to_{ClickHouseTable.GT_STATS.value}`
        """,
    )


def copy_existing_project_partitions(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable,
    project_guids: list[str],
):
    client = get_clickhouse_client()
    existing_project_guids = {
        r[0]
        for r in client.execute(
            f"""
        SELECT partition
        FROM system.parts
        WHERE table = `{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}`
        AND database = `{Env.CLICKHOUSE_DATABASE}`
        AND partition IN %(project_guids)s;
        """,
            {'project_guids': project_guids},
        )
    }
    run_id_hash = hashlib.sha256(
        run_id.encode('utf8'),
    ).hexdigest()
    for project_guid in existing_project_guids:
        client.execute(
            f"""
            ALTER TABLE {STAGING_CLICKHOUSE_DATABASE}.`{run_id_hash}`/`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}`
            ATTACH PARTITION %s(project_guid) FROM {Env.CLICKHOUSE_DATABASE}.`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}`
            """,
            {'project_guid': project_guid},
        )
        client.execute(
            f"""
            ALTER TABLE {STAGING_CLICKHOUSE_DATABASE}.`{run_id_hash}`/`{reference_genome.value}/{dataset_type.value}/{ClickHouseTable.GT_STATS.value}`
            ATTACH PARTITION %s(project_guid) FROM {Env.CLICKHOUSE_DATABASE}.`{reference_genome.value}/{dataset_type.value}/{ClickHouseTable.GT_STATS.value}`
            """,
            {'project_guid': project_guid},
        )


def delete_existing_families(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable,
    family_guids: list[str],
) -> None:
    pass


def insert_new_entries(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable,
) -> None:
    pass


def copy_new_project_partitions(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable,
    project_guids: list[str],
) -> None:
    pass


def direct_insert(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable,
) -> None:
    client = get_clickhouse_client()
    key = max_src_key(
        reference_genome,
        dataset_type,
        run_id,
        clickhouse_table,
    )
    if dst_key_exists(
        reference_genome,
        dataset_type,
        clickhouse_table,
        key,
    ):
        msg = f'Skipping direct insert of `{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}` as {clickhouse_table.key_field}={key} already exists'
        logger.info(msg)
        return
    path = clickhouse_insert_table_fn(
        clickhouse_table.src_path_fn(reference_genome, dataset_type, run_id),
    )
    client.execute(
        f"""
        INSERT INTO {Env.CLICKHOUSE_DATABASE}.`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}`
        SELECT {clickhouse_table.select_fields}
        FROM {path}
        ORDER BY {clickhouse_table.key_field} ASC
        """,
    )


def atomic_entries_insert(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable.ENTRIES,
) -> None:
    drop_staging_db()
    create_staging_entries(
        reference_genome,
        dataset_type,
        run_id,
        clickhouse_table,
    )
    with hfs.open(
        metadata_for_run_path(
            reference_genome,
            dataset_type,
            run_id,
        ),
        'r',
    ) as f:
        metadata_json = json.load(f.read())
        project_guids = metadata_json['project_guids']
        family_guids = metadata_json['family_samples'].keys()
    copy_existing_project_partitions(
        reference_genome,
        dataset_type,
        run_id,
        clickhouse_table,
        project_guids,
    )
    delete_existing_families(
        reference_genome,
        dataset_type,
        run_id,
        clickhouse_table,
        family_guids,
    )
    insert_new_entries(
        reference_genome,
        dataset_type,
        run_id,
        clickhouse_table,
    )
    copy_new_project_partitions(
        reference_genome,
        dataset_type,
        run_id,
        clickhouse_table,
        project_guids,
    )


@retry()
def insert(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
    clickhouse_table: ClickHouseTable,
) -> None:
    if clickhouse_table == ClickHouseTable.ENTRIES:
        atomic_entries_insert(
            reference_genome,
            dataset_type,
            run_id,
            clickhouse_table,
        )
    else:
        direct_insert(
            reference_genome,
            dataset_type,
            run_id,
            clickhouse_table,
        )


def clickhouse_insert_table_fn(path: str):
    path = os.path.join(path, '*.parquet')
    if path.startswith('gcs://'):
        return f"gcs('{path.replace('gcs://', GOOGLE_XML_API_PATH)}', '{Env.CLICKHOUSE_GCS_HMAC_KEY}', '{Env.CLICKHOUSE_GCS_HMAC_SECRET}', 'Parquet')"
    return f"file('{path}', 'Parquet')"


def get_clickhouse_client() -> Client:
    return Client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        user=Env.CLICKHOUSE_USER,
        **{'password': Env.CLICKHOUSE_PASSWORD} if Env.CLICKHOUSE_PASSWORD else {},
    )
