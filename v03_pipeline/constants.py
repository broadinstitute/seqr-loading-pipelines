from enum import Enum

SEQR_DATASETS = 'gs://seqr-datasets'
SEQR_LOADING_TEMP = 'gs://seqr-loading-temp'
SEQR_SCRATCH_TEMP = 'gs://seqr-scratch-temp'
V03 = 'v03'

class DatasetType(Enum):
    GCNV = 'GCNV'
    MITO = 'MITO'
    SNV = 'SNV'
    SV = 'SV'

class Env(Enum):
    DEV = 'dev'
    PROD = 'prod'

class ReferenceGenome(Enum):
    GRCh37 = 'GRCh37'
    GRCh38 = 'GRCh38'
