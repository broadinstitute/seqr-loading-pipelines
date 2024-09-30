import os
from dataclasses import dataclass

# NB: using os.environ.get inside the dataclass defaults gives a lint error.
GRCH37_TO_GRCH38_LIFTOVER_REF_PATH = os.environ.get(
    'GRCH37_TO_GRCH38_LIFTOVER_REF_PATH',
    'gs://hail-common/references/grch37_to_grch38.over.chain.gz',
)
GRCH38_TO_GRCH37_LIFTOVER_REF_PATH = os.environ.get(
    'GRCH38_TO_GRCH37_LIFTOVER_REF_PATH',
    'gs://hail-common/references/grch38_to_grch37.over.chain.gz',
)
HAIL_TMP_DIR = os.environ.get('HAIL_TMP_DIR', '/tmp')  # noqa: S108
HAIL_SEARCH_DATA_DIR = os.environ.get(
    'HAIL_SEARCH_DATA_DIR', '/seqr/seqr-hail-search-data'
)
LOADING_DATASETS_DIR = os.environ.get('LOADING_DATASETS_DIR', '/seqr/seqr-loading-temp')
PRIVATE_REFERENCE_DATASETS_DIR = os.environ.get(
    'PRIVATE_REFERENCE_DATASETS_DIR',
    '/seqr/seqr-reference-data-private',
)
REFERENCE_DATASETS_DIR = os.environ.get(
    'REFERENCE_DATASETS_DIR',
    '/seqr/seqr-reference-data',
)
VEP_REFERENCE_DATASETS_DIR = os.environ.get(
    'VEP_REFERENCE_DATASETS_DIR',
    '/seqr/vep-reference-data',
)

# Allele registry secrets :/
ALLELE_REGISTRY_SECRET_NAME = os.environ.get('ALLELE_REGISTRY_SECRET_NAME', None)
PROJECT_ID = os.environ.get('PROJECT_ID', None)

# Feature Flags
ACCESS_PRIVATE_REFERENCE_DATASETS = (
    os.environ.get('ACCESS_PRIVATE_REFERENCE_DATASETS') == '1'
)
CHECK_SEX_AND_RELATEDNESS = os.environ.get('CHECK_SEX_AND_RELATEDNESS') == '1'
EXPECT_WES_FILTERS = os.environ.get('EXPECT_WES_FILTERS') == '1'
SHOULD_REGISTER_ALLELES = os.environ.get('SHOULD_REGISTER_ALLELES') == '1'


@dataclass
class Env:
    ACCESS_PRIVATE_REFERENCE_DATASETS: bool = ACCESS_PRIVATE_REFERENCE_DATASETS
    ALLELE_REGISTRY_SECRET_NAME: str | None = ALLELE_REGISTRY_SECRET_NAME
    CHECK_SEX_AND_RELATEDNESS: bool = CHECK_SEX_AND_RELATEDNESS
    EXPECT_WES_FILTERS: bool = EXPECT_WES_FILTERS
    HAIL_TMP_DIR: str = HAIL_TMP_DIR
    HAIL_SEARCH_DATA_DIR: str = HAIL_SEARCH_DATA_DIR
    GRCH37_TO_GRCH38_LIFTOVER_REF_PATH: str = GRCH37_TO_GRCH38_LIFTOVER_REF_PATH
    GRCH38_TO_GRCH37_LIFTOVER_REF_PATH: str = GRCH38_TO_GRCH37_LIFTOVER_REF_PATH
    LOADING_DATASETS_DIR: str = LOADING_DATASETS_DIR
    PRIVATE_REFERENCE_DATASETS_DIR: str = PRIVATE_REFERENCE_DATASETS_DIR
    PROJECT_ID: str | None = PROJECT_ID
    REFERENCE_DATASETS_DIR: str = REFERENCE_DATASETS_DIR
    SHOULD_REGISTER_ALLELES: bool = SHOULD_REGISTER_ALLELES
    VEP_REFERENCE_DATASETS_DIR: str = VEP_REFERENCE_DATASETS_DIR
