import importlib
import types
from enum import Enum

import hail as hl

from v03_pipeline.lib.model import AccessControl, DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.reference_datasets import clinvar
from v03_pipeline.lib.reference_datasets.misc import (
    filter_contigs,
    get_enum_select_fields,
)

DATASET_TYPES = 'dataset_types'
VERSION = 'version'
RAW_DATASET_PATH = 'raw_dataset_path'
ENUMS = 'enums'
MODULE_NAME = 'module_name'


class BaseReferenceDataset:
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

    def version(self, reference_genome: ReferenceGenome) -> str:
        version = CONFIG[self][reference_genome][VERSION]
        if isinstance(version, types.FunctionType):
            return version(
                self.raw_dataset_path(reference_genome),
            )
        return version

    @property
    def enums(self) -> dict | None:
        return CONFIG[self].get(ENUMS)

    @property
    def enum_globals(self) -> hl.Struct:
        if self.enums:
            return hl.Struct(**self.enums)
        return hl.missing(hl.tstruct(hl.tstr, hl.tarray(hl.tstr)))

    def raw_dataset_path(self, reference_genome: ReferenceGenome) -> str | list[str]:
        return CONFIG[self][reference_genome][RAW_DATASET_PATH]

    def get_ht(
        self,
        reference_genome: ReferenceGenome,
    ) -> hl.Table:
        module_file_name = CONFIG[self].get(MODULE_NAME, self.name)
        module = importlib.import_module(
            f'v03_pipeline.lib.reference_datasets.{module_file_name}',
        )
        path = self.raw_dataset_path(reference_genome)
        ht = module.get_ht(path, reference_genome)
        if self.enums:
            ht = ht.transmute(**get_enum_select_fields(ht, self.enums))
        ht = filter_contigs(ht, reference_genome)
        return ht.annotate_globals(
            version=self.version(reference_genome),
            enums=self.enum_globals,
        )


class ReferenceDataset(BaseReferenceDataset, str, Enum):
    cadd = 'cadd'
    clinvar = 'clinvar'
    dbnsfp = 'dbnsfp'
    hgmd = 'hgmd'
    mitimpact = 'mitimpact'
    topmed = 'topmed'
    gnomad_exomes = 'gnomad_exomes'
    gnomad_genomes = 'gnomad_genomes'
    gnomad_qc = 'gnomad_qc'


class ReferenceDatasetQuery(BaseReferenceDataset, str, Enum):
    clinvar_path = 'clinvar_path'
    high_af_variants = 'high_af_variants'

    @property
    def requires(self) -> ReferenceDataset:
        return {
            self.clinvar_path: ReferenceDataset.clinvar,
            self.high_af_variants: ReferenceDataset.gnomad_genomes,
        }[self]


CONFIG = {
    ReferenceDataset.dbnsfp: {
        ENUMS: {
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
    ReferenceDataset.clinvar: {
        ENUMS: clinvar.ENUMS,
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
    ReferenceDataset.topmed: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            # NB: TopMed data is available to download via https://legacy.bravo.sph.umich.edu/freeze8/hg38/downloads/vcf/<chrom>
            # However, users must be authenticated and accept TOS to access it so for now we will host a copy of the data
            RAW_DATASET_PATH: 'gs://seqr-reference-data/GRCh38/TopMed/bravo-dbsnp-all.vcf.gz',
        },
    },
    ReferenceDataset.mitimpact: {
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.MITO]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'https://mitimpact.css-mendel.it/cdn/MitImpact_db_3.1.3.txt.zip',
        },
    },
    ReferenceDataset.hgmd: {
        ENUMS: {'class': ['DM', 'DM?', 'DP', 'DFP', 'FP', 'R']},
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://seqr-reference-data-private/GRCh37/HGMD/HGMD_Pro_2023.1_hg19.vcf.gz',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_Pro_2023.1_hg38.vcf.gz',
        },
    },
    ReferenceDataset.gnomad_exomes: {
        MODULE_NAME: 'gnomad',
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://gcp-public-data--gnomad/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://gcp-public-data--gnomad/release/4.1/ht/exomes/gnomad.exomes.v4.1.sites.ht',
        },
    },
    ReferenceDataset.gnomad_genomes: {
        MODULE_NAME: 'gnomad',
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://gcp-public-data--gnomad/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://gcp-public-data--gnomad/release/4.1/ht/genomes/gnomad.genomes.v4.1.sites.ht',
        },
    },
    ReferenceDataset.gnomad_qc: {
        ReferenceGenome.GRCh37: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://seqr-reference-data/gnomad_qc/GRCh37/gnomad.joint.high_callrate_common_biallelic_snps.pruned.mt',
        },
        ReferenceGenome.GRCh38: {
            DATASET_TYPES: frozenset([DatasetType.SNV_INDEL]),
            VERSION: '1.0',
            RAW_DATASET_PATH: 'gs://gcp-public-data--gnomad/release/4.0/pca/gnomad.v4.0.pca_loadings.ht',
        },
    },
}
CONFIG[ReferenceDatasetQuery.clinvar_path] = CONFIG[ReferenceDataset.clinvar]
