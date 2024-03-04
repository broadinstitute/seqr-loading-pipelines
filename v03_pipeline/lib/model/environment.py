import os
from dataclasses import dataclass

# NB: using os.environ.get inside the dataclass defaults gives a lint error.
ACCESS_PRIVATE_REFERENCE_DATASETS = (
    os.environ.get('ACCESS_PRIVATE_REFERENCE_DATASETS') == '1'
)
CHECK_SEX_AND_RELATEDNESS = os.environ.get('CHECK_SEX_AND_RELATEDNESS') == '1'
REFERENCE_DATA_AUTO_UPDATE = os.environ.get('REFERENCE_DATA_AUTO_UPDATE') == '1'
HAIL_TMPDIR = os.environ.get('HAIL_TMPDIR', '/tmp')  # noqa: S108
HAIL_SEARCH_DATA = os.environ.get('HAIL_SEARCH_DATA', '/hail-search-data')
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


@dataclass
class Env:
    ACCESS_PRIVATE_REFERENCE_DATASETS: bool = ACCESS_PRIVATE_REFERENCE_DATASETS
    CHECK_SEX_AND_RELATEDNESS: bool = CHECK_SEX_AND_RELATEDNESS
    REFERENCE_DATA_AUTO_UPDATE: bool = REFERENCE_DATA_AUTO_UPDATE
    HAIL_TMPDIR: str = HAIL_TMPDIR
    HAIL_SEARCH_DATA: str = HAIL_SEARCH_DATA
    LOADING_DATASETS: str = LOADING_DATASETS
    PRIVATE_REFERENCE_DATASETS: str = PRIVATE_REFERENCE_DATASETS
    REFERENCE_DATASETS: str = REFERENCE_DATASETS
    VEP_CONFIG_PATH: str | None = VEP_CONFIG_PATH
    VEP_CONFIG_URI: str | None = VEP_CONFIG_URI
