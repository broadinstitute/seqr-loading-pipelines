import hail as hl


def get_ht(raw_dataset_path: str, *_) -> hl.Table:
    ht = hl.read_table(raw_dataset_path)
    return ht.select(Eigen_phred=ht.info['Eigen-phred'])
