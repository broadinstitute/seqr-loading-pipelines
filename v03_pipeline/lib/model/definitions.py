from enum import Enum

import hail as hl


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


class Sex(Enum):
    FEMALE = 'F'
    MALE = 'M'
    UNKNOWN = 'U'


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
    def hl_reference(self) -> hl.ReferenceGenome:
        return hl.get_reference(self.value)

    @property
    def standard_contigs(self) -> set[str]:
        return {
            *self.hl_reference.contigs[:25],
        }

    @property
    def optional_contigs(self) -> set[str]:
        return {
            ReferenceGenome.GRCh37: {
                'Y',
                'MT',
            },
            ReferenceGenome.GRCh38: {
                'chrY',
                'chrM',
            },
        }[self]

    def contig_recoding(self, include_mt: bool = False) -> dict[str, str]:
        recode = {
            ReferenceGenome.GRCh37: {
                f'chr{i}': f'{i}' for i in ([*list(range(1, 23)), 'X', 'Y'])
            },
            ReferenceGenome.GRCh38: {
                f'{i}': f'chr{i}' for i in ([*list(range(1, 23)), 'X', 'Y'])
            },
        }[self]

        if include_mt and self == ReferenceGenome.GRCh38:
            recode.update({'MT': 'chrM'})
        if include_mt and self == ReferenceGenome.GRCh37:
            recode.update({'chrM': 'MT'})

        return recode


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
