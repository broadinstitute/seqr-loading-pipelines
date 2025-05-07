from collections.abc import Callable
from enum import StrEnum

from clickhouse_driver import Client

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.retry import retry
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import (
    new_clinvar_variants_parquet_path,
    new_transcripts_parquet_path,
    new_variants_parquet_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
)

logger = get_logger(__name__)

GOOGLE_XML_API_PATH = 'https://storage.googleapis.com/'


class ClickHouseTable(StrEnum):
    ANNOTATIONS_DISK = 'annotations_disk'
    ANNOTATIONS_MEMORY = 'annotations_memory'
    CLINVAR = 'clinvar'
    KEY_LOOKUP = 'key_lookup'
    TRANSCRIPTS = 'transcripts'

    @property
    def src_path_fn(self) -> Callable:
        return {
            ClickHouseTable.ANNOTATIONS_DISK: new_variants_parquet_path,
            ClickHouseTable.ANNOTATIONS_MEMORY: new_variants_parquet_path,
            ClickHouseTable.CLINVAR: new_clinvar_variants_parquet_path,
            ClickHouseTable.KEY_LOOKUP: new_variants_parquet_path,
            ClickHouseTable.TRANSCRIPTS: new_transcripts_parquet_path,
        }[self]

    def should_load(self, reference_genome: ReferenceGenome, dataset_type: DatasetType):
        return (
            self != ClickHouseTable.CLINVAR
            or ReferenceDataset.clinvar
            in BaseReferenceDataset.for_reference_genome_dataset_type(
                reference_genome,
                dataset_type,
            )
        )

    @property
    def dst_key_field(self):
        return 'variant_id' if self == ClickHouseTable.KEY_LOOKUP else 'key'

    def src_key_field(self, dataset_type):
        if self == ClickHouseTable.KEY_LOOKUP:
            if dataset_type in {DatasetType.GCNV, DatasetType.SV}:
                return 'variant_id'
            return "concat(chrom, '-', toString(pos), '-', ref, '-', alt)"
        return 'key'

    def select_fields(self, dataset_type):
        if self == ClickHouseTable.KEY_LOOKUP:
            return f'{self.src_key_field(dataset_type)}, key'
        return '*'


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
            WHERE {clickhouse_table.dst_key_field} = %(key)s
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
    key_field = clickhouse_table.src_key_field(dataset_type)
    return client.execute(
        f"""
        SELECT max({key_field}) FROM {path}
        """,
    )[0][0]


@retry()
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
        msg = f'Skipping direct insert of `{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}` as {clickhouse_table.dst_key_field}={key} already exists'
        logger.info(msg)
        return
    path = clickhouse_insert_table_fn(
        clickhouse_table.src_path_fn(reference_genome, dataset_type, run_id),
    )
    client.execute(
        f"""
        INSERT INTO {Env.CLICKHOUSE_DATABASE}.`{reference_genome.value}/{dataset_type.value}/{clickhouse_table.value}`
        SELECT {clickhouse_table.select_fields(dataset_type)}
        FROM {path}
        ORDER BY {clickhouse_table.src_key_field(dataset_type)} ASC
        """,
    )


def clickhouse_insert_table_fn(path: str):
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
