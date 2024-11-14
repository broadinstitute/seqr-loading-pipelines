import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import vcf_to_ht

RENAME = {
    'info.AC#': 'AC',
    'info.AF#': 'AF',
    'info.AN': 'AN',
    'info.Hom#': 'Hom',
    'info.Het#': 'Het',
}


# adapted from download_and_create_reference_datasets/v02/create_ht__topmed.py
def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = vcf_to_ht(raw_dataset_path, reference_genome)
    return ht.rename(**RENAME).select(*RENAME.values())
