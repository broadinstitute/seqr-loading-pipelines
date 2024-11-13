import importlib
import types
from enum import Enum

import hail as hl

from v03_pipeline.lib.model import AccessControl, DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.reference_datasets import clinvar

DATASET_TYPES = 'dataset_types'
VERSION = 'version'
RAW_DATASET_PATH = 'raw_dataset_path'
ENUM_SELECT = 'enum_select'


class ReferenceDataset(str, Enum):
    cadd = 'cadd'
    clinvar = 'clinvar'
    dbnsfp = 'dbnsfp'
    hgmd = 'hgmd'

    @classmethod
    def for_reference_genome_dataset_type(
        cls,
        reference_genome: ReferenceGenome,
        dataset_type: DatasetType,
    ) -> list['ReferenceDataset']:
        reference_datasets = [
            dataset
            for dataset, config in CONFIG.items()
            if dataset_type in config.get(reference_genome, {}).get(DATASET_TYPES)
        ]
        if not Env.ACCESS_PRIVATE_REFERENCE_DATASETS:
            return [
                dataset
                for dataset in reference_datasets
                if dataset.access_control == AccessControl.PUBLIC
            ]
        return reference_datasets

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDataset.hgmd:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    @property
    def enum_select(self) -> dict:
        return CONFIG[self].get(ENUM_SELECT)


    def version(self, reference_genome: ReferenceGenome) -> str:
        version = CONFIG[self][reference_genome][VERSION]
        if isinstance(version, types.FunctionType):
            return version(
                self.raw_dataset_path(reference_genome),
            )
        return version

    def raw_dataset_path(self, reference_genome: ReferenceGenome) -> str | list[str]:
        return CONFIG[self][reference_genome][RAW_DATASET_PATH]

    def get_ht(self, reference_genome: ReferenceGenome) -> hl.Table:
        module = importlib.import_module(
            f'v03_pipeline.lib.reference_datasets.{self.name}',
        )
        path = self.raw_dataset_path(reference_genome)
        return module.get_ht(path, reference_genome)


CONFIG = {
    ReferenceDataset.cadd: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: [
                'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/whole_genome_SNVs.tsv.gz',
                'https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh37/gnomad.genomes-exomes.r4.0.indel.tsv.gz',
            ],
        },
    },
    ReferenceDataset.clinvar: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: clinvar.parse_clinvar_release_date,
            RAW_DATASET_PATH: 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL, DatasetType.MITO]),
            VERSION: clinvar.parse_clinvar_release_date,
            RAW_DATASET_PATH: 'https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
        },
    },
    ReferenceDataset.dbnsfp: {
        ENUM_SELECT: {
            'MutationTaster_pred': ['D', 'A', 'N', 'P'],
        },
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'https://dbnsfp.s3.amazonaws.com/dbNSFP4.7a.zip',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL, DatasetType.MITO]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'https://dbnsfp.s3.amazonaws.com/dbNSFP4.7a.zip',
        },
    },
}
