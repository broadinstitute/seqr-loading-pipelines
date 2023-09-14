import os
from dataclasses import dataclass

DATASETS = os.environ.get('DATASETS_ROOT', '/seqr-datasets')
HAIL_TMPDIR = os.environ.get('HAIL_TMPDIR_ROOT', '/tmp')  # noqa: S108
LOADING_DATASETS = os.environ.get('LOADING_DATASETS_ROOT', '/seqr-loading-temp')
PRIVATE_REFERENCE_DATASETS = os.environ.get(
    'PRIVATE_REFERENCE_DATASETS_ROOT',
    '/seqr-reference-data-private',
)
REFERENCE_DATASETS = os.environ.get(
    'REFERENCE_DATASETS_ROOT',
    '/seqr-reference-data',
)


@dataclass
class Env:
    ACCESS_PRIVATE_DATASETS: bool = os.environ.get('ACCESS_PRIVATE_DATASETS') == '1'
    DATASETS: str = DATASETS
    HAIL_TMPDIR: str = HAIL_TMPDIR
    LOADING_DATASETS: str = LOADING_DATASETS
    MOCK_VEP: bool = os.environ.get('MOCK_VEP') == '1'
    PRIVATE_REFERENCE_DATASETS: str = PRIVATE_REFERENCE_DATASETS
    REFERENCE_DATASETS: str = REFERENCE_DATASETS
    RUN_SEX_AND_RELATEDNESS: bool = os.environ.get('RUN_SEX_AND_RELATEDNESS') == '1'
