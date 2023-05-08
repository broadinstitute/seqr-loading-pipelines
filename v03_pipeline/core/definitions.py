from enum import Enum
from typing import Set


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
    # Shared
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

    def reference_datasets(self) -> Set[ReferenceDataset]:
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


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
