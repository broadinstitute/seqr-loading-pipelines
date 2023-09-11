from __future__ import annotations

from enum import Enum

import hail as hl


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
        hl_reference = hl.get_reference(self.value)
        return {
            *hl_reference.x_contigs,
            *hl_reference.y_contigs,
        }

    @property
    def standard_contigs(self) -> set[str]:
        hl_reference = hl.get_reference(self.value)
        return {
            *hl_reference.contigs[:25],
        }


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
