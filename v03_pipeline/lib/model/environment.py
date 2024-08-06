import os
from dataclasses import dataclass

# NB: using os.environ.get inside the dataclass defaults gives a lint error.
HAIL_TMPDIR = os.environ.get('HAIL_TMPDIR', '/tmp')  # noqa: S108
HAIL_SEARCH_DATA = os.environ.get('HAIL_SEARCH_DATA', '/hail-search-data')
GRCH37_TO_GRCH38_LIFTOVER_REF_PATH = os.environ.get(
    'GRCH37_TO_GRCH38_LIFTOVER_REF_PATH',
    'gs://hail-common/references/grch37_to_grch38.over.chain.gz',
)
GRCH38_TO_GRCH37_LIFTOVER_REF_PATH = os.environ.get(
    'GRCH38_TO_GRCH37_LIFTOVER_REF_PATH',
    'gs://hail-common/references/grch38_to_grch37.over.chain.gz',
)
LOADING_DATASETS = os.environ.get('LOADING_DATASETS', '/seqr-loading-temp')
PRIVATE_REFERENCE_DATASETS = os.environ.get(
    'PRIVATE_REFERENCE_DATASETS',
    '/seqr-reference-data-private',
)
REFERENCE_DATASETS = os.environ.get(
    'REFERENCE_DATASETS',
    '/seqr-reference-data',
)
VEP_CONFIG_PATH = os.environ.get('VEP_CONFIG_PATH', None)
VEP_CONFIG_URI = os.environ.get('VEP_CONFIG_URI', None)

# Allele registry secrets :/
ALLELE_REGISTRY_SECRET_NAME = os.environ.get('ALLELE_REGISTRY_SECRET_NAME', None)
PROJECT_ID = os.environ.get('PROJECT_ID', None)

# Feature Flags
ACCESS_PRIVATE_REFERENCE_DATASETS = (
    os.environ.get('ACCESS_PRIVATE_REFERENCE_DATASETS') == '1'
)
CHECK_SEX_AND_RELATEDNESS = os.environ.get('CHECK_SEX_AND_RELATEDNESS') == '1'
EXPECT_WES_FILTERS = os.environ.get('EXPECT_WES_FILTERS') == '1'
REFERENCE_DATA_AUTO_UPDATE = os.environ.get('REFERENCE_DATA_AUTO_UPDATE') == '1'
SHOULD_REGISTER_ALLELES = os.environ.get('SHOULD_REGISTER_ALLELES') == '1'


@dataclass
class Env:
    ACCESS_PRIVATE_REFERENCE_DATASETS: bool = ACCESS_PRIVATE_REFERENCE_DATASETS
    ALLELE_REGISTRY_SECRET_NAME: str | None = ALLELE_REGISTRY_SECRET_NAME
    CHECK_SEX_AND_RELATEDNESS: bool = CHECK_SEX_AND_RELATEDNESS
    EXPECT_WES_FILTERS: bool = EXPECT_WES_FILTERS
    HAIL_TMPDIR: str = HAIL_TMPDIR
    HAIL_SEARCH_DATA: str = HAIL_SEARCH_DATA
    GRCH37_TO_GRCH38_LIFTOVER_REF_PATH: str = GRCH37_TO_GRCH38_LIFTOVER_REF_PATH
    GRCH38_TO_GRCH37_LIFTOVER_REF_PATH: str = GRCH38_TO_GRCH37_LIFTOVER_REF_PATH
    LOADING_DATASETS: str = LOADING_DATASETS
    PRIVATE_REFERENCE_DATASETS: str = PRIVATE_REFERENCE_DATASETS
    PROJECT_ID: str | None = PROJECT_ID
    REFERENCE_DATA_AUTO_UPDATE: bool = REFERENCE_DATA_AUTO_UPDATE
    REFERENCE_DATASETS: str = REFERENCE_DATASETS
    SHOULD_REGISTER_ALLELES: bool = SHOULD_REGISTER_ALLELES
    VEP_CONFIG_PATH: str | None = VEP_CONFIG_PATH
    VEP_CONFIG_URI: str | None = VEP_CONFIG_URI
