from __future__ import annotations

from enum import Enum


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


class DatasetType(Enum):
    GCNV = 'GCNV'
    MITO = 'MITO'
    SNV = 'SNV'
    SV = 'SV'

    @property
    def sample_types(self) -> set[SampleType]:
        return {
            DatasetType.GCNV: {SampleType.WES},
            DatasetType.MITO: {SampleType.WGS},  # is this right?
            DatasetType.SNV: {SampleType.WES, SampleType.WGS},
            DatasetType.SV: {SampleType.WGS},
        }[self]


class DataRoot(Enum):
    LOCAL_DATASETS = '/seqr-datasets'
    LOCAL_REFERENCE_DATA = '/seqr-reference-data'
    SEQR_DATASETS = 'gs://seqr-datasets'
    SEQR_LOADING_TEMP = 'gs://seqr-loading-temp'
    SEQR_REFERENCE_DATA = 'gs://seqr-reference-data'
    SEQR_REFERENCE_DATA_PRIVATE = 'gs://seqr-reference-data-private'
    SEQR_SCRATCH_TEMP = 'gs://seqr-scratch-temp'


class Env(Enum):
    DEV = 'dev'
    LOCAL = 'local'
    PROD = 'prod'


class PipelineVersion(Enum):
    V02 = 'v02'
    V03 = 'v03'


class ReferenceGenome(Enum):
    GRCh37 = 'GRCh37'
    GRCh38 = 'GRCh38'


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
