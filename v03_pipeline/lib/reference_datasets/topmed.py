import hail as hl

from v03_pipeline.lib.misc.nested_field import parse_nested_field
from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import vcf_to_ht

SELECT = {
    'AC': 'info.AC#',
    'AF': 'info.AF#',
    'AN': 'info.AN',
    'Hom': 'info.Hom#',
    'Het': 'info.Het#',
}


# adapted from download_and_create_reference_datasets/v02/create_ht__topmed.py
def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = vcf_to_ht(raw_dataset_path, reference_genome)
    return ht.select(
        **{k: parse_nested_field(ht, v) for k, v in SELECT.items()},
    )