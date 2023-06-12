from __future__ import annotations

from typing import Any

import hail as hl


def hgmd(
    ht: hl.Table,
    hgmd_ht: hl.Table | None = None,
    **_: Any,
) -> hl.Expression | None:
    if hgmd_ht is None:
        return None
    return hgmd_ht[ht.key].hgmd


def gnomad_non_coding_constraint(
    ht: hl.Table,
    interval_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.Struct(
        z_score=(
            interval_ht.index(ht.locus, all_matches=True)
            .filter(
                lambda x: hl.is_defined(x.gnomad_non_coding_constraint['z_score']),
            )
            .gnomad_non_coding_constraint.z_score.first()
        ),
    )


def screen(
    ht: hl.Table,
    interval_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.Struct(
        region_type_ids=(
            interval_ht.index(
                ht.locus,
                all_matches=True,
            ).flatmap(
                lambda x: x.screen['region_type_ids'],
            )
        ),
    )
