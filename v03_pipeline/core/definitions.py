from __future__ import annotations

from enum import Enum


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


class DatasetType(Enum):
    GCNV = 'gcnv'
    MITO = 'mito'
    SNV = 'snv'
    SV = 'sv'


class Env(Enum):
    DEV = 'dev'
    PROD = 'prod'


class GCSBucket(Enum):
    SEQR_DATASETS = 'gs://seqr-datasets'
    SEQR_LOADING_TEMP = 'gs://seqr-loading-temp'
    SEQR_REFERENCE_DATA = 'gs://seqr-reference-data'
    SEQR_REFERENCE_DATA_PRIVATE = 'gs://seqr-reference-data-private'
    SEQR_SCRATCH_TEMP = 'gs://seqr-scratch-temp'


class PipelineVersion(Enum):
    V02 = 'v02'
    V03 = 'v03'


class ReferenceDataset(Enum):
    # Shared (used by SNV and MITO)
    CLINVAR = 'clinvar'
    DBNSFP = 'dbnsfp'

    # SNV
    CADD = 'cadd'
    EIGEN = 'eigen'
    EXAC = 'exac'
    GENO2MP = 'geno2mp'
    GNOMAD_EXOME_COVERAGE = 'gnomad_exome_coverage'
    GNOMAD_EXOMES = 'gnomad_exomes'
    GNOMAD_GENOME_COVERAGE = 'gnomad_genome_coverage'
    GNOMAD_GENOMES = 'gnomad_genomes'
    GNOMAD_NON_CODING_CONSTRAINT = 'gnomad_non_coding_constraint'
    HGMD = 'hgmd'
    MPC = 'mpc'
    PRIMATE_AI = 'primate_ai'
    SCREEN = 'screen'
    SPLICE_AI = 'splice_ai'
    TGP = 'tgp'
    TOPMED = 'topmed'

    # MITO
    GNOMAD_MITO = 'gnomad_mito'
    HELIX_MITO = 'helix_mito'
    HMTVAR = 'hmtvar'
    MITOMAP = 'mitomap'
    MITIMPACT = 'mitimpact'


class ReferenceDatasetCollection(Enum):
    CLINVAR = 'clinvar'
    COMBINED = 'combined'
    COMBINED_MITO = 'combined_mito'
    HGMD = 'hgmd'
    INTERVAL_REFERENCE = 'interval_reference'

    @property
    def access_control(self) -> AccessControl:
        if self == ReferenceDatasetCollection.HGMD:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    @property
    def dataset_types(self) -> set[DatasetType]:
        return {
            ReferenceDatasetCollection.CLINVAR: {DatasetType.MITO, DatasetType.SNV},
            ReferenceDatasetCollection.COMBINED: {DatasetType.SNV},
            ReferenceDatasetCollection.COMBINED_MITO: {DatasetType.MITO},
            ReferenceDatasetCollection.HGMD: {DatasetType.SNV},
            ReferenceDatasetCollection.INTERVAL_REFERENCE: {DatasetType.SNV},
        }[self]

    @property
    def reference_datasets(self) -> set[ReferenceDataset]:
        return {
            ReferenceDatasetCollection.CLINVAR: {ReferenceDataset.CLINVAR},
            ReferenceDatasetCollection.COMBINED: {
                ReferenceDataset.CADD,
                ReferenceDataset.DBNSFP,
                ReferenceDataset.EIGEN,
                ReferenceDataset.EXAC,
                ReferenceDataset.GENO2MP,
                ReferenceDataset.GNOMAD_EXOME_COVERAGE,
                ReferenceDataset.GNOMAD_EXOMES,
                ReferenceDataset.GNOMAD_GENOME_COVERAGE,
                ReferenceDataset.GNOMAD_GENOMES,
                ReferenceDataset.MPC,
                ReferenceDataset.PRIMATE_AI,
                ReferenceDataset.SPLICE_AI,
                ReferenceDataset.TGP,
                ReferenceDataset.TOPMED,
            },
            ReferenceDatasetCollection.COMBINED_MITO: {
                ReferenceDataset.DBNSFP,
                ReferenceDataset.GNOMAD_MITO,
                ReferenceDataset.HELIX_MITO,
                ReferenceDataset.HMTVAR,
                ReferenceDataset.MITOMAP,
                ReferenceDataset.MITIMPACT,
            },
            ReferenceDatasetCollection.HGMD: {
                ReferenceDataset.HGMD,
            },
            ReferenceDatasetCollection.INTERVAL_REFERENCE: {
                ReferenceDataset.GNOMAD_NON_CODING_CONSTRAINT,
                ReferenceDataset.SCREEN,
            },
        }[self]


class ReferenceGenome(Enum):
    GRCh37 = 'GRCh37'
    GRCh38 = 'GRCh38'


class SampleSource(Enum):
    ANVIL = 'anvil'
    RDG_BROAD_EXTERNAL = 'rdg_broad_external'
    RDG_BROAD_INTERNAL = 'rdg_broad_internal'


class SampleType(Enum):
    WES = 'wes'
    WGS = 'wgs'


class ValidationDatasetCollection(Enum):
    SAMPLE_TYPE_VALIDATION = 'sample_type_validation'
