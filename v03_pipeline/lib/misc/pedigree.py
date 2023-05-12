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
        s=ht.Individual_ID,
        family_id=ht.Family_ID,
    )
    return ht.key_by(ht.s)

def families_missing_samples(pedigree_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    ht = pedigree_ht.anti_join(samples_ht)
    ht = ht.join(pedigree_ht)
    ht = ht.key_by(ht.family_id)
    ht = ht.distinct()
    return ht.select()

def families_to_include(pedigree_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    pt = pedigree_ht.key_by(pedigree_ht.family_id)
    ht = families_missing_samples(pedigree_ht, samples_ht)
    ht = pt.anti_join(ht)
    ht = ht.distinct()
    return ht.select()

def samples_to_include(pedigree_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    pt = pedigree_ht.key_by(pedigree_ht.family_id)
    ht = families_to_include(pedigree_ht, samples_ht)
    ht = pt.join(ht)
    return ht.select(ht.s)
