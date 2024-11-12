from enum import Enum
from types import MappingProxyType

from v03_pipeline.lib.model import AccessControl, DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.reference_datasets.cadd import load_cadd_ht_from_raw_dataset
from v03_pipeline.lib.reference_datasets.hgmd import (
    HGMD_ENUM_SELECT,
    download_and_import_hgmd_vcf,
)

LOAD_DATASET_FUNC = 'load_parsed_dataset_func'
VERSION = 'version'
RAW_DATASET_PATH = 'raw_dataset_path'
ENUM_SELECT = 'enum_select'


class ReferenceDataset(str, Enum):
    cadd = 'cadd'
    hgmd = 'hgmd'

    CONFIG = MappingProxyType(
        {
            'cadd': {
                LOAD_DATASET_FUNC: load_cadd_ht_from_raw_dataset,
                ReferenceGenome.GRCh37: {
                    VERSION: '1.0',
                    RAW_DATASET_PATH: [
                        'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/whole_genome_SNVs.tsv.gz',
                        'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/gnomad.genomes.r4.0.indel.tsv.gz',
                    ],
                },
                ReferenceGenome.GRCh38: {
                    VERSION: '1.0',
                    RAW_DATASET_PATH: [
                        'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/whole_genome_SNVs.tsv.gz',
                        'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/gnomad.genomes-exomes.r4.0.indel.tsv.gz',
                    ],
                },
            },
            'hgmd': {
                LOAD_DATASET_FUNC: download_and_import_hgmd_vcf,
                ReferenceGenome.GRCh37: {
                    VERSION: '1.0',
                    RAW_DATASET_PATH: 'gs://seqr-reference-data/v3.1/GRCh37/reference_datasets/hgmd-v2020.1.vcf.bgz',
                    ENUM_SELECT: HGMD_ENUM_SELECT,
                },
                ReferenceGenome.GRCh38: {
                    VERSION: '1.0',
                    RAW_DATASET_PATH: 'gs://seqr-reference-data/v3.1/GRCh38/reference_datasets/hgmd-v2020.1.vcf.bgz',
                    ENUM_SELECT: HGMD_ENUM_SELECT,
                },
            },
        },
    )

    @classmethod
    def for_reference_genome_dataset_type(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> list['ReferenceDataset']:
        reference_datasets = {
            (ReferenceGenome.GRCh37, DatasetType.SNV_INDEL): [
                ReferenceDataset.cadd,
                ReferenceDataset.hgmd,
            ],
            (ReferenceGenome.GRCh38, DatasetType.SNV_INDEL): [
                ReferenceDataset.cadd,
                ReferenceDataset.hgmd,
            ],
        }.get((reference_genome, dataset_type), [])
        if not Env.ACCESS_PRIVATE_REFERENCE_DATASETS:
            return [
                rd
                for rd in reference_datasets
                if rd.access_control == AccessControl.PUBLIC
            ]
        return reference_datasets

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDataset.hgmd:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    def version(self, reference_genome: ReferenceGenome) -> str:
        return self.CONFIG[self.name][reference_genome][VERSION]

    def raw_dataset_path(self, reference_genome: ReferenceGenome) -> str | list[str]:
        return self.CONFIG[self.name][reference_genome][RAW_DATASET_PATH]

    def load_parsed_dataset_func(self) -> callable:
        return self.CONFIG[self.name][LOAD_DATASET_FUNC]
