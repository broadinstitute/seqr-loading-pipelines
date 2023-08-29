import os
from typing import TypedDict


class DataRoot(TypedDict):
    DATASETS: str = os.environ.get('DATASETS_ROOT', '/seqr-datasets')
    HAIL_TMPDIR: str = os.environ.get('HAIL_TMPDIR_ROOT', '/tmp')  # noqa: S108
    LOADING_DATASETS: str = os.environ.get(
        'LOADING_DATASETS_ROOT',
        '/seqr-loading-temp',
    )
    PRIVATE_REFERENCE_DATASETS: str = os.environ.get(
        'PRIVATE_REFERENCE_DATASETS_ROOT',
        '/seqr-reference-data-private',
    )
    REFERENCE_DATASETS: str = os.environ.get(
        'REFERENCE_DATASETS_ROOT',
        '/seqr-reference-data',
    )


class Env(TypedDict):
    ACCESS_PRIVATE_DATASETS: bool = os.environ.get('ACCESS_PRIVATE_DATASETS') == '1'
    MOCK_VEP: bool = os.environ.get('MOCK_VEP') == '1'
