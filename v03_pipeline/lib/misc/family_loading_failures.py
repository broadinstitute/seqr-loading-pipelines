from __future__ import annotations
import hail as hl

from v03_pipeline.lib.misc.pedigree import Family
from v03_pipeline.lib.model import Ploidy


def build_relatedness_check_lookup(
    relatedness_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> dict[tuple[str, str], hl.Struct]:
    # Build relatedness check lookup
    relatedness_check_ht = relatedness_check_ht.key_by(
        i=remap_lookup.get(relatedness_check_ht.i, relatedness_check_ht.i),
        j=remap_lookup.get(relatedness_check_ht.j, relatedness_check_ht.j),
    )
    return {(r.i, r.j): r.drop('i', 'j') for r in relatedness_check_ht.collect()}


def build_sex_check_lookup(
    sex_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> dict[str, Ploidy]:
    # Build sex check lookup
    sex_check_ht = sex_check_ht.key_by(
        s=remap_lookup.get(sex_check_ht.s, sex_check_ht.s),
    )
    sex_check_ht = sex_check_ht.select('sex')
    return {r.s: Ploidy(r.sex) for r in sex_check_ht.collect()}


def get_families_failed_missing_samples(
    mt: hl.MatrixTable,
    families: set[Family],
) -> set[Family]:
    callset_samples = set(mt.cols().s.collect())
    return {
        family
        for family in families
        if len(family.samples.keys() - callset_samples) > 0
    }


def get_families_failed_relatedness_check(
    families: set[Family],
    relatedness_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> set[Family]:
    build_relatedness_check_lookup(
        relatedness_check_ht,
        remap_lookup,
    )
    failed_families = set()
    return failed_families


def get_families_failed_sex_check(
    families: set[Family],
    sex_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> set[Family]:
    sex_check_lookup = build_sex_check_lookup(sex_check_ht, remap_lookup)
    return {
        family
        for family in families
        for sample_id in family.samples
        if family.samples[sample_id].sex != sex_check_lookup[sample_id]
    }
