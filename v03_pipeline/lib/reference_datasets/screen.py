import hail as hl

from v03_pipeline.lib.reference_datasets.misc import get_enum_select_fields

REGION_TYPES = [
    'CTCF-bound',
    'CTCF-only',
    'DNase-H3K4me3',
    'PLS',
    'dELS',
    'pELS',
    'DNase-only',
    'low-DNase',
]

ENUMS = {'region_type': REGION_TYPES}


def get_ht(raw_dataset_path: str, *_) -> hl.Table:
    ht = hl.read_table(raw_dataset_path)
    ht = ht.select(region_type=ht.target)
    return ht.transmute(**get_enum_select_fields(ht, ENUMS))
