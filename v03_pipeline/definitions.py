from enum import Enum


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


class DatasetType(Enum):
    GCNV = 'GCNV'
    MITO = 'MITO'
    SNV = 'SNV'
    SV = 'SV'


class Env(Enum):
    DEV = 'dev'
    PROD = 'prod'


class ReferenceDataset(Enum):
    CLINVAR = 'clinvar'
    DBNSFP = 'dbnsfp'


class MITOReferenceDataset(ReferenceDataset):
    GNOMAD_MITO = 'gnomad_mito'
    HELIX_MITO = 'helix_mito'
    HMTVAR = 'hmtvar'
    MITOMAP = 'mitomap'
    MITIMPACT = 'mitimpact'


class SNVReferenceDataset(ReferenceDataset):
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


class ReferenceDatasetCollection(Enum):
    CLINVAR = 'clinvar'
    COMBINED = 'combined'
    COMBINED_MITO = 'combined_mito'
    HGMD = 'hgmd'
    INTERVAL_REFERENCE = 'interval_reference'

    def access_control(self) -> AccessControl:
        if self == ReferenceDatasetCollection.HGMD:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC


class ReferenceGenome(Enum):
    GRCh37 = 'GRCh37'
    GRCh38 = 'GRCh38'


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
