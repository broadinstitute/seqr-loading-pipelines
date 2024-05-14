from enum import Enum

import hail as hl


class AccessControl(Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


class Sex(Enum):
    FEMALE = 'F'
    MALE = 'M'


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

    @property
    def allele_registry_vcf_header(self) -> list[str]:
        return {
            ReferenceGenome.GRCh37: [
                '##fileformat=VCFv4.2\n',
                '##contig=<ID=1,length=249250621,assembly=GRCh37>\n',
                '##contig=<ID=2,length=243199373,assembly=GRCh37>\n',
                '##contig=<ID=3,length=198022430,assembly=GRCh37>\n',
                '##contig=<ID=4,length=191154276,assembly=GRCh37>\n',
                '##contig=<ID=5,length=180915260,assembly=GRCh37>\n',
                '##contig=<ID=6,length=171115067,assembly=GRCh37>\n',
                '##contig=<ID=7,length=159138663,assembly=GRCh37>\n',
                '##contig=<ID=8,length=146364022,assembly=GRCh37>\n',
                '##contig=<ID=9,length=141213431,assembly=GRCh37>\n',
                '##contig=<ID=10,length=135534747,assembly=GRCh37>\n',
                '##contig=<ID=11,length=135006516,assembly=GRCh37>\n',
                '##contig=<ID=12,length=133851895,assembly=GRCh37>\n',
                '##contig=<ID=13,length=115169878,assembly=GRCh37>\n',
                '##contig=<ID=14,length=107349540,assembly=GRCh37>\n',
                '##contig=<ID=15,length=102531392,assembly=GRCh37>\n',
                '##contig=<ID=16,length=90354753,assembly=GRCh37>\n',
                '##contig=<ID=17,length=81195210,assembly=GRCh37>\n',
                '##contig=<ID=18,length=78077248,assembly=GRCh37>\n',
                '##contig=<ID=19,length=59128983,assembly=GRCh37>\n',
                '##contig=<ID=20,length=63025520,assembly=GRCh37>\n',
                '##contig=<ID=21,length=48129895,assembly=GRCh37>\n',
                '##contig=<ID=22,length=51304566,assembly=GRCh37>\n',
                '##contig=<ID=X,length=155270560,assembly=GRCh37>\n',
                '##contig=<ID=Y,length=59373566,assembly=GRCh37>\n',
                '##contig=<ID=MT,length=16569,assembly=GRCh37>\n',
                '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n',
            ],
            ReferenceGenome.GRCh38: [
                '##fileformat=VCFv4.2\n',
                '##contig=<ID=1,length=248956422,assembly=GRCh38>\n',
                '##contig=<ID=2,length=242193529,assembly=GRCh38>\n',
                '##contig=<ID=3,length=198295559,assembly=GRCh38>\n',
                '##contig=<ID=4,length=190214555,assembly=GRCh38>\n',
                '##contig=<ID=5,length=181538259,assembly=GRCh38>\n',
                '##contig=<ID=6,length=170805979,assembly=GRCh38>\n',
                '##contig=<ID=7,length=159345973,assembly=GRCh38>\n',
                '##contig=<ID=8,length=145138636,assembly=GRCh38>\n',
                '##contig=<ID=9,length=138394717,assembly=GRCh38>\n',
                '##contig=<ID=10,length=133797422,assembly=GRCh38>\n',
                '##contig=<ID=11,length=135086622,assembly=GRCh38>\n',
                '##contig=<ID=12,length=133275309,assembly=GRCh38>\n',
                '##contig=<ID=13,length=114364328,assembly=GRCh38>\n',
                '##contig=<ID=14,length=107043718,assembly=GRCh38>\n',
                '##contig=<ID=15,length=101991189,assembly=GRCh38>\n',
                '##contig=<ID=16,length=90338345,assembly=GRCh38>\n',
                '##contig=<ID=17,length=83257441,assembly=GRCh38>\n',
                '##contig=<ID=18,length=80373285,assembly=GRCh38>\n',
                '##contig=<ID=19,length=58617616,assembly=GRCh38>\n',
                '##contig=<ID=20,length=64444167,assembly=GRCh38>\n',
                '##contig=<ID=21,length=46709983,assembly=GRCh38>\n',
                '##contig=<ID=22,length=50818468,assembly=GRCh38>\n',
                '##contig=<ID=X,length=156040895,assembly=GRCh38>\n',
                '##contig=<ID=Y,length=57227415,assembly=GRCh38>\n',
                '##contig=<ID=M,length=16569,assembly=GRCh38>\n',
                '#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO\n',
            ],
        }[self]

    @property
    def allele_registry_gnomad_id(self) -> str:
        return {
            ReferenceGenome.GRCh37: 'gnomAD_2',
            ReferenceGenome.GRCh38: 'gnomAD_4',
        }[self]


class SampleType(Enum):
    WES = 'WES'
    WGS = 'WGS'
