import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import vcf_to_ht
from v03_pipeline.lib.misc.nested_field import parse_nested_field

SELECT = {
    'AF_POPMAX': 'info.AF_POPMAX',
    'AF': 'info.AF#',
    'AC_Adj': 'info.AC_Adj#',
    'AC_Het': 'info.AC_Het#',
    'AC_Hom': 'info.AC_Hom#',
    'AC_Hemi': 'info.AC_Hemi#',
    'AN_Adj': 'info.AN_Adj',
}


def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = vcf_to_ht(raw_dataset_path, reference_genome, split_multi=True)
    return ht.select(
        **{k: parse_nested_field(ht, v) for k, v in SELECT.items()},
    )
