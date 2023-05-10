from __future__ import annotations

from enum import Enum


class AccessControl(Enum):
    PUBLIC = 'PUBLIC'
    PRIVATE = 'PRIVATE'


class DatasetType(Enum):
    GCNV = 'GCNV'
    MITO = 'MITO'
    SNV = 'SNV'
    SV = 'SV'


class Env(Enum):
    DEV = 'DEV'
    LOCAL = 'LOCAL'
    PROD = 'PROD'


class ReferenceDataset(Enum):
    # Shared (used by SNV and MITO)
    CLINVAR = 'CLINVAR'
    DBNSFP = 'DBNSFP'

    # SNV
    CADD = 'CADD'
    EIGEN = 'EIGEN'
    EXAC = 'EXAC'
    GENO2MP = 'GENO2MP'
    GNOMAD_EXOME_COVERAGE = 'GNOMAD_EXOME_COVERAGE'
    GNOMAD_EXOMES = 'GNOMAD_EXOMES'
    GNOMAD_GENOME_COVERAGE = 'GNOMAD_GENOME_COVERAGE'
    GNOMAD_GENOMES = 'GNOMAD_GENOMES'
    GNOMAD_NON_CODING_CONSTRAINT = 'GNOMAD_NON_CODING_CONSTRAINT'
    HGMD = 'HGMD'
    MPC = 'MPC'
    PRIMATE_AI = 'PRIMATE_AI'
    SCREEN = 'SCREEN'
    SPLICE_AI = 'SPLICE_AI'
    TGP = 'TGP'
    TOPMED = 'TOPMED'

    # MITO
    GNOMAD_MITO = 'GNOMAD_MITO'
    HELIX_MITO = 'HELIX_MITO'
    HMTVAR = 'HMTVAR'
    MITOMAP = 'MITOMAP'
    MITIMPACT = 'MITIMPACT'


class ReferenceDatasetCollection(Enum):
    CLINVAR = 'CLINVAR'
    COMBINED = 'COMBINED'
    COMBINED_MITO = 'COMBINED_MITO'
    HGMD = 'HGMD'
    INTERVAL_REFERENCE = 'INTERVAL_REFERENCE'

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
        }

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
    ANVIL = 'ANVIL'
    RDG_BROAD_EXTERNAL = 'RDG_BROAD_EXTERNAL'
    RDG_BROAD_INTERNAL = 'RDG_BROAD_INTERNAL'


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'


class ValidationDatasetCollection(Enum):
    VARIANT_VALIDATION = 'VARIANT_VALIDATION'
