import hail as hl


def get_ht(raw_dataset_path: str, *_) -> hl.Table:
    if raw_dataset_path.endswith('mt'):
        ht = hl.read_matrix_table(raw_dataset_path).rows()
    else:
        ht = hl.read_table(raw_dataset_path)

    return ht
