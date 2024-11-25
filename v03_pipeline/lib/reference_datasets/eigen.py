import hail as hl


def get_ht(path: str, *_) -> hl.Table:
    ht = hl.read_table(path)
    return ht.select(Eigen_phred=ht.info['Eigen-phred'])
