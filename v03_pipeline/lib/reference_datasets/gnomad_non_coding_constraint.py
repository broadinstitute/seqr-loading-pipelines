import hail as hl


def get_ht(raw_dataset_path: str, *_) -> hl.Table:
    ht = hl.read_table(raw_dataset_path)
    return ht.select(z_score=hl.float32(ht.target))
