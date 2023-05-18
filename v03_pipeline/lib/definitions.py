from __future__ import annotations

from enum import Enum

import hail as hl


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


class DatasetType(Enum):
    GCNV = 'GCNV'
    MITO = 'MITO'
    SNV = 'SNV'
    SV = 'SV'

    def variant_annotations_table_key_type(
        self,
        reference_genome: ReferenceGenome,
    ) -> hl.tstruct:
        default_key = hl.tstruct(
            locus=hl.tlocus(reference_genome.value),
            alleles=hl.tarray(hl.tstr),
        )
        return {
            DatasetType.GCNV: hl.tstruct(variant_name=hl.tstr, svtype=hl.tstr),
            DatasetType.SV: hl.tstruct(rsid=hl.tstr),
        }.get(self, default_key)


class DataRoot(Enum):
    TEST_DATASETS = 'test-datasets'
    TEST_REFERENCE_DATA = 'test-reference-data'
    LOCAL_DATASETS = 'seqr-datasets'
    LOCAL_REFERENCE_DATA = 'seqr-reference-data'
    SEQR_DATASETS = 'gs://seqr-datasets'
    SEQR_LOADING_TEMP = 'gs://seqr-loading-temp'
    SEQR_REFERENCE_DATA = 'gs://seqr-reference-data'
    SEQR_REFERENCE_DATA_PRIVATE = 'gs://seqr-reference-data-private'
    SEQR_SCRATCH_TEMP = 'gs://seqr-scratch-temp'


class Env(Enum):
    DEV = 'dev'
    LOCAL = 'local'
    PROD = 'prod'
    TEST = 'test'


class PipelineVersion(Enum):
    V02 = 'v02'
    V03 = 'v03'


class ReferenceGenome(Enum):
    GRCh37 = 'GRCh37'
    GRCh38 = 'GRCh38'


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
