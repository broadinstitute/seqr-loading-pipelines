import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import vcf_to_ht

RENAME = {
    'AF_POPMAX': 'info.AF_POPMAX',
    'AF': 'info.AF#',
    'AC_Adj': 'info.AC_Adj#',
    'AC_Het': 'info.AC_Het#',
    'AC_Hom': 'info.AC_Hom#',
    'AC_Hemi': 'info.AC_Hemi#',
    'AN_Adj': 'info.AN_Adj',
}


# adapted from download_and_create_reference_datasets/v02/create_ht__topmed.py
def get_ht(raw_dataset_path: str, reference_genome: ReferenceGenome) -> hl.Table:
    ht = vcf_to_ht(raw_dataset_path, reference_genome)
    return ht.rename(**RENAME).select(*RENAME.values())
