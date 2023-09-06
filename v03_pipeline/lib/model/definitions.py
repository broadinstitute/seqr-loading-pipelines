from __future__ import annotations

from enum import Enum


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


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
    def autosomes(self) -> set[str]:
        return {
            ReferenceGenome.GRCh37: {str(x) for x in range(23)},
            ReferenceGenome.GRCh38: {f'chr{x}' for x in range(23)},
        }[self]

    @property
    def sex_chromosomes(self) -> set[str]:
        return {
            ReferenceGenome.GRCh37: {
                'X',
                'Y',
            },
            ReferenceGenome.GRCh38: {
                'chrX',
                'chrY',
            },
        }[self]

    @property
    def standard_contigs(self) -> set[str]:
        return {
            ReferenceGenome.GRCh37: {
                *self.autosomes,
                *self.sex_chromosomes,
                'MT',
            },
            ReferenceGenome.GRCh38: {
                *self.autosomes,
                *self.sex_chromosomes,
                'chrM',
            },
        }[self]


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
