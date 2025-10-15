import hail as hl

from v03_pipeline.lib.core import ReferenceGenome
from v03_pipeline.lib.misc.nested_field import parse_nested_field
from v03_pipeline.lib.reference_datasets.misc import vcf_to_ht

SELECT = {
    'AF': 'info.AF#',
    'AC_Adj': 'info.AC_Adj#',
    'AC_Het': 'info.AC_Het#',
    'AC_Hom': 'info.AC_Hom#',
    'AC_Hemi': 'info.AC_Hemi#',
    'AN_Adj': 'info.AN_Adj',
}


def get_ht(path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = vcf_to_ht(path, reference_genome, split_multi=True)
    return ht.select(
        AF_POPMAX=hl.or_missing(
            ht.info.AC_POPMAX[ht.a_index - 1] != 'NA',
            hl.float32(
                hl.int32(ht.info.AC_POPMAX[ht.a_index - 1])
                / hl.int32(ht.info.AN_POPMAX[ht.a_index - 1]),
            ),
        ),
        **{k: parse_nested_field(ht, v) for k, v in SELECT.items()},
    )
