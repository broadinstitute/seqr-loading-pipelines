from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from v03_pipeline.lib.misc.pedigree import Relation
from v03_pipeline.lib.model import Ploidy

if TYPE_CHECKING:
    import hail as hl

    from v03_pipeline.lib.misc.pedigree import Family, Sample


def passes_relatedness_check(
    relatedness_check_lookup: dict[tuple[str, str], list],
    sample_id: str,
    other_id: str,
    relation: Relation,
) -> bool:
    # No relationship to check, return true
    if other_id is None:
        return True
    coefficients = relatedness_check_lookup.get(
        # NB: sibling, half_sibling, and aunt/uncle relationships are
        # guaranteed to be identified with sample_id < other_id so this is unnecessary.
        # Parent & Grandparent relationships, however, are not guarnanteed.
        (min(sample_id, other_id), max(sample_id, other_id)),
    )
    if not coefficients:
        return False
    return np.allclose(
        coefficients,
        relation.coefficients,
        5e-2,
    )


def passes_all_relatedness_checks(  # noqa: C901
    relatedness_check_lookup: dict[tuple[str, str], list],
    sample: Sample,
) -> bool:
    for parent_id in [sample.mother, sample.father]:
        if not passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            parent_id,
            Relation.PARENT,
        ):
            return False

    for grandparent_id in [
        sample.maternal_grandmother,
        sample.maternal_grandfather,
        sample.paternal_grandmother,
        sample.paternal_grandfather,
    ]:
        if not passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            grandparent_id,
            Relation.GRANDPARENT,
        ):
            return False

    for sibling_id in sample.siblings:
        if not passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            sibling_id,
            Relation.SIBLING,
        ):
            return False

    for half_sibling_id in sample.half_siblings:
        # NB: A "half sibling" parsed from the pedigree may actually be a sibling, so we allow those
        # through as well.
        if not passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            half_sibling_id,
            Relation.HALF_SIBLING,
        ) and not passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            half_sibling_id,
            Relation.SIBLING,
        ):
            return False

    for aunt_uncle_id in sample.aunt_uncles:
        if not passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            aunt_uncle_id,
            Relation.AUNT_UNCLE,
        ):
            return False
    return True


def build_relatedness_check_lookup(
    relatedness_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> dict[tuple[str, str], list]:
    # Build relatedness check lookup
    relatedness_check_ht = relatedness_check_ht.key_by(
        i=remap_lookup.get(relatedness_check_ht.i, relatedness_check_ht.i),
        j=remap_lookup.get(relatedness_check_ht.j, relatedness_check_ht.j),
    )
    return {
        (r.i, r.j): list(r.drop('i', 'j').values())
        for r in relatedness_check_ht.collect()
    }


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
    failed_families = set()
    for family in families:
        if len(family.samples.keys() - callset_samples) > 0:
            failed_families.add(family)
    return failed_families


def get_families_failed_relatedness_check(
    families: set[Family],
    relatedness_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> set[Family]:
    relatedness_check_lookup = build_relatedness_check_lookup(
        relatedness_check_ht,
        remap_lookup,
    )
    failed_families = set()
    for family in families:
        for sample in family.samples.values():
            if not passes_all_relatedness_checks(relatedness_check_lookup, sample):
                failed_families.add(family)
                break
    return failed_families


def get_families_failed_sex_check(
    families: set[Family],
    sex_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> set[Family]:
    sex_check_lookup = build_sex_check_lookup(sex_check_ht, remap_lookup)
    failed_families = set()
    for family in families:
        for sample_id in family.samples:
            if family.samples[sample_id].sex != sex_check_lookup[sample_id]:
                failed_families.add(family)
                break
    return failed_families
