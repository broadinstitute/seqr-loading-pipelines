import hail as hl


def select_fields(ht: hl.Table) -> hl.Table:
    selects = {}
    selects['AN'] = hl.int32(ht.AN)
    selects['AC_hom'] = hl.int32(ht.AC_hom)
    selects['AC_het'] = hl.int32(ht.AC_het)
    selects['AF_hom'] = ht.AF_hom
    selects['AF_het'] = ht.AF_het
    selects['max_hl'] = ht.max_hl
    return ht.select(**selects)


def get_ht(raw_dataset_path: str, *_) -> hl.Table:
    ht = hl.read_table(raw_dataset_path)
    ht = select_fields(ht)
    return ht.select_globals()
