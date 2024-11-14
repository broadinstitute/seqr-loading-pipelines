import hail as hl


def get_ht(raw_dataset_path: str, *_) -> hl.Table:
    ht = hl.read_table(raw_dataset_path)
    ht = ht.select(
        AN=hl.int32(ht.AN),
        AC_hom=hl.int32(ht.AC_hom),
        AC_het=hl.int32(ht.AC_het),
        AF_hom=ht.AF_hom,
        AF_het=ht.AF_het,
        max_hl=ht.max_hl,
    )
    return ht.select_globals()
