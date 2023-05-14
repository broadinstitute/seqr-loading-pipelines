import hail as hl


def import_pedigree(pedigree_path: str) -> hl.Table:
    ht = hl.import_table(
        pedigree_path,
        types={
            'Project_GUID': hl.tstr,
            'Family_ID': hl.tstr,
            'Individual_ID': hl.tstr,
            'Paternal_ID': hl.tstr,
            'Maternal_ID': hl.tstr,
            'Sex': hl.tstr,
        },
    )
    ht = ht.select(
        family_id=ht.Family_ID,
        s=ht.Individual_ID,
    )
    return ht.key_by(ht.family_id)

def families_to_exclude(pedigree_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    ht = pedigree_ht.key_by(pedigree_ht.s).anti_join(samples_ht)
    ht = ht.key_by(ht.family_id)
    ht = ht.distinct()
    return ht.select()

def families_to_include(pedigree_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    ht = pedigree_ht.anti_join(
        families_to_exclude(pedigree_ht, samples_ht)
    )
    ht = ht.distinct()
    return ht.select()

def samples_to_include(pedigree_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    ht = pedigree_ht.join(
        families_to_include(pedigree_ht, samples_ht)
    )
    ht = ht.key_by(ht.s)
    return ht.select()
