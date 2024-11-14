import hail as hl


def get_ht(raw_dataset_path: str, *_) -> hl.Table:
    ht = hl.read_table(raw_dataset_path)
    return ht.select(region_type=ht.target)
