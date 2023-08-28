from __future__ import annotations

import os
from enum import Enum


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


class DataRoot(Enum):
    DATASETS = os.environ.get('DATASETS_ROOT', 'seqr-datasets')
    HAIL_TMPDIR = os.environ.get('HAIL_TMPDIR_ROOT', '/tmp')  # noqa: S108
    LOADING_DATASETS = os.environ.get(
        'LOADING_DATASETS_ROOT',
        'seqr-loading-temp',
    )
    PRIVATE_REFERENCE_DATASETS = os.environ.get(
        'PRIVATE_REFERENCE_DATASETS_ROOT',
        'seqr-reference-data-private',
    )
    REFERENCE_DATASETS = os.environ.get('REFERENCE_DATASETS_ROOT', 'seqr-reference-data')


class Env(Enum):
    ACCESS_PRIVATE_DATASETS: bool = os.environ.get('ACCESS_PRIVATE_DATASETS') == '1'
    MOCK_VEP: bool = os.environ.get('MOCK_VEP') == '1'


class PipelineVersion(Enum):
    V02 = 'v02'
    V03 = 'v03'


class ReferenceGenome(Enum):
    GRCh37 = 'GRCh37'
    GRCh38 = 'GRCh38'

    @property
    def v02_value(self) -> str:
        return self.value[-2:]

    @property
    def standard_contigs(self) -> set[str]:
        return {
            ReferenceGenome.GRCh37: {
                '1',
                '10',
                '11',
                '12',
                '13',
                '14',
                '15',
                '16',
                '17',
                '18',
                '19',
                '2',
                '20',
                '21',
                '22',
                '3',
                '4',
                '5',
                '6',
                '7',
                '8',
                '9',
                'X',
                'Y',
                'MT',
            },
            ReferenceGenome.GRCh38: {
                'chr1',
                'chr10',
                'chr11',
                'chr12',
                'chr13',
                'chr14',
                'chr15',
                'chr16',
                'chr17',
                'chr18',
                'chr19',
                'chr2',
                'chr20',
                'chr21',
                'chr22',
                'chr3',
                'chr4',
                'chr5',
                'chr6',
                'chr7',
                'chr8',
                'chr9',
                'chrX',
                'chrY',
                'chrM',
            },
        }[self]


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
