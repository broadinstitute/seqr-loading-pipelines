import hail as hl

from v03_pipeline.lib.misc.pedigree import Family
from v03_pipeline.lib.model import Ploidy


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
    relatedness_check_lookup: dict[tuple[str, str], hl.Struct],
) -> set[Family]:
    failed_families = set()
    return failed_families


def get_families_failed_sex_check(
    families: set[Family],
    sex_check_lookup: dict[str, Ploidy],
) -> set[Family]:
    failed_families = set()
    for family in families:
        for sample_id in family.samples:
            if family.samples[sample_id].sex != sex_check_lookup[sample_id]:
                failed_families.add(family)
                continue
    return failed_families
