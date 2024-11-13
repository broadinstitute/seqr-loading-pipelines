import hail as hl


def get_ht(raw_dataset_path: str, *args) -> hl.Table:
    if raw_dataset_path.endswith('mt'):
        return hl.read_matrix_table(raw_dataset_path).rows()
    return hl.read_table(raw_dataset_path)
