from collections import defaultdict

import hail as hl
import numpy as np

from v03_pipeline.lib.misc.pedigree import Family, Relation, Sample
from v03_pipeline.lib.model import Sex

RELATEDNESS_TOLERANCE = 0.2


def passes_relatedness_check(
    relatedness_check_lookup: dict[tuple[str, str], list],
    sample_id: str,
    other_id: str,
    relation: Relation,
) -> tuple[bool, str | None]:
    # No relationship to check, return true
    if other_id is None:
        return True, None
    coefficients = relatedness_check_lookup.get(
        (min(sample_id, other_id), max(sample_id, other_id)),
    )
    if not coefficients or not np.allclose(
        coefficients,
        relation.coefficients,
        atol=RELATEDNESS_TOLERANCE,
    ):
        return (
            False,
            f'Sample {sample_id} has expected relation "{relation.value}" to {other_id} but has coefficients {coefficients or []}',
        )
    return True, None


def all_relatedness_checks(  # noqa: C901
    relatedness_check_lookup: dict[tuple[str, str], list],
    sample: Sample,
) -> list[str]:
    failure_reasons = []
    for parent_id in [sample.mother, sample.father]:
        success, reason = passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            parent_id,
            Relation.PARENT,
        )
        if not success:
            failure_reasons.append(reason)

    for grandparent_id in [
        sample.maternal_grandmother,
        sample.maternal_grandfather,
        sample.paternal_grandmother,
        sample.paternal_grandfather,
    ]:
        success, reason = passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            grandparent_id,
            Relation.GRANDPARENT,
        )
        if not success:
            failure_reasons.append(reason)

    for sibling_id in sample.siblings:
        success, reason = passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            sibling_id,
            Relation.SIBLING,
        )
        if not success:
            failure_reasons.append(reason)

    for half_sibling_id in sample.half_siblings:
        # NB: A "half sibling" parsed from the pedigree may actually be a sibling, so we allow those
        # through as well.
        success1, _ = passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            half_sibling_id,
            Relation.SIBLING,
        )
        success2, reason = passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            half_sibling_id,
            Relation.HALF_SIBLING,
        )
        if not success1 and not success2:
            failure_reasons.append(reason)

    for aunt_nephew_id in sample.aunt_nephews:
        success, reason = passes_relatedness_check(
            relatedness_check_lookup,
            sample.sample_id,
            aunt_nephew_id,
            Relation.AUNT_NEPHEW,
        )
        if not success:
            failure_reasons.append(reason)
    return failure_reasons


def build_relatedness_check_lookup(
    relatedness_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> dict[tuple[str, str], list]:
    relatedness_check_ht = relatedness_check_ht.key_by(
        i=remap_lookup.get(relatedness_check_ht.i, relatedness_check_ht.i),
        j=remap_lookup.get(relatedness_check_ht.j, relatedness_check_ht.j),
    )
    return {
        # NB: samples are sorted in the original ibd but not necessarily
        # sorted after remapping
        (min(r.i, r.j), max(r.i, r.j)): list(r.drop('i', 'j').values())
        for r in relatedness_check_ht.collect()
    }


def build_sex_check_lookup(
    sex_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> dict[str, Sex]:
    sex_check_ht = sex_check_ht.key_by(
        s=remap_lookup.get(sex_check_ht.s, sex_check_ht.s),
    )
    sex_check_ht = sex_check_ht.select('predicted_sex')
    return {r.s: Sex(r.predicted_sex) for r in sex_check_ht.collect()}


def get_families_failed_missing_samples(
    mt: hl.MatrixTable,
    families: set[Family],
) -> dict[Family, list[str]]:
    callset_samples = set(mt.cols().s.collect())
    failed_families = {}
    for family in families:
        missing_samples = family.samples.keys() - callset_samples
        if len(missing_samples) > 0:
            # NB: This is an array of a single element for consistency with
            # the other checks.
            failed_families[family] = [f'Missing samples: {missing_samples}']
    return failed_families


def get_families_failed_relatedness_check(
    families: set[Family],
    relatedness_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> dict[Family, list[str]]:
    relatedness_check_lookup = build_relatedness_check_lookup(
        relatedness_check_ht,
        remap_lookup,
    )
    failed_families = defaultdict(list)
    for family in families:
        for sample in family.samples.values():
            failure_reasons = all_relatedness_checks(
                relatedness_check_lookup,
                sample,
            )
            if failure_reasons:
                failed_families[family].extend(failure_reasons)
    return dict(failed_families)


def get_families_failed_sex_check(
    families: set[Family],
    sex_check_ht: hl.Table,
    remap_lookup: hl.dict,
) -> dict[Family, list[str]]:
    sex_check_lookup = build_sex_check_lookup(sex_check_ht, remap_lookup)
    failed_families = defaultdict(list)
    for family in families:
        for sample_id in family.samples:
            if family.samples[sample_id].sex != sex_check_lookup[sample_id]:
                failed_families[family].append(
                    f'Sample {sample_id} has pedigree sex {family.samples[sample_id].sex.value} but imputed sex {sex_check_lookup[sample_id].value}',
                )
    return dict(failed_families)
