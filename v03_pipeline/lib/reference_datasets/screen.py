import hail as hl

SCREEN_REGION_TYPES = [
    'CTCF-bound',
    'CTCF-only',
    'DNase-H3K4me3',
    'PLS',
    'dELS',
    'pELS',
    'DNase-only',
    'low-DNase',
]


def get_ht(raw_dataset_path: str, *_) -> hl.Table:
    ht = hl.read_table(raw_dataset_path)
    return ht.select(region_type=ht.target)
