from enum import StrEnum
from types import Callable

from clickhouse_driver import Client

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

GOOGLE_XML_API_PATH = 'https://storage.googleapis.com/'


class ClickhouseTable(StrEnum):
    ANNOTATIONS_DISK = 'annotations_disk'
    ANNOTATIONS_MEMORY = 'annotations_memory'
    CLINVAR = 'clinvar'
    TRANSCRIPTS = 'transcripts'

    @property
    def src_path_fn(self) -> Callable:
        return {
            ClickhouseTable.ANNOTATIONS_DISK: new_variants_parquet_path,
            ClickhouseTable.ANNOTATIONS_MEMORY: new_variants_parquet_path,
            ClickhouseTable.CLINVAR: new_clinvar_variants_parquet_path,
            ClickhouseTable.TRANSCRIPTS: new_transcripts_parquet_path,
        }[self]

    def should_load(self, reference_genome: ReferenceGenome, dataset_type: DatasetType):
        return (
            ReferenceDataset.clinvar
            in BaseReferenceDataset.for_reference_genome_dataset_type(
                reference_genome,
                dataset_type,
            )
        )


def google_xml_native_path(path: str):
    if path.startswith('gcs://'):
        return path.replace('gcs://', GOOGLE_XML_API_PATH)
    return path


def get_clickhouse_client() -> Client:
    return Client(
        host=Env.CLICKHOUSE_SERVICE_HOSTNAME,
        port=Env.CLICKHOUSE_SERVICE_PORT,
        user=Env.CLICKHOUSE_USER,
        **{'password': Env.CLICKHOUSE_PASSWORD} if Env.CLICKHOUSE_PASSWORD else {},
    )
