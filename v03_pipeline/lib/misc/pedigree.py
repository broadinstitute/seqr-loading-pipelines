from __future__ import annotations

import itertools
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import hail as hl


def families_to_exclude(pedigree_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    ht = pedigree_ht.key_by(pedigree_ht.s).anti_join(samples_ht)
    ht = ht.key_by(ht.family_guid)
    ht = ht.distinct()
    return ht.select()


def families_to_include(pedigree_ht: hl.Table, samples_ht: hl.Table) -> hl.Table:
    ht = pedigree_ht.anti_join(families_to_exclude(pedigree_ht, samples_ht))
    ht = ht.distinct()
    return ht.select()


def samples_to_include(
    pedigree_ht: hl.Table,
    families_to_include_ht: hl.Table,
) -> hl.Table:
    ht = pedigree_ht.join(families_to_include_ht)
    ht = ht.key_by(ht.s)
    return ht.select()


def expected_relations(
    pedigree_ht: hl.Table,
    families_to_include_ht: hl.Table,
) -> dict[str, set[tuple[str, str]]]:
    ht = pedigree_ht.join(families_to_include_ht)
    relations = {}
    for family_guid, rows_grouper in itertools.groupby(
        ht.collect(), lambda x: x.family_guid,
    ):
        rows = list(rows_grouper)
        relations[family_guid] = list(
            itertools.combinations(sorted([row.s for row in rows]), 2),
        )

        # Find and remove parents, since Mother and Father should not be related:
        parents = [
            (row.maternal_s, row.paternal_s)
            for row in rows
            if row.maternal_s and row.paternal_s
        ]
        if parents:
            relations[family_guid].remove((min(parents[0]), max(parents[0])))
    return relations
