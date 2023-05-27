from __future__ import annotations

import hail as hl


def hgmd(
    mt: hl.MatrixTable,
    hgmd_ht: hl.Table | None,
    **_,
) -> hl.Expression | None:
    if not hgmd_ht:
        return None
    return hgmd_ht[mt.row_key].hgmd


def gnomad_non_coding_constraint(
    mt: hl.MatrixTable,
    interval_reference_ht: hl.Table,
    **_,
) -> hl.Expression:
    return hl.Struct(
        z_score=(
            interval_reference_ht.index(mt.locus, all_matches=True)
            .filter(
                lambda x: hl.is_defined(x.gnomad_non_coding_constraint['z_score']),
            )
            .gnomad_non_coding_constraint.z_score.first()
        ),
    )


def screen(
    mt: hl.MatrixTable,
    interval_reference_ht: hl.Table,
    **_,
) -> hl.Expression:
    return hl.Struct(
        region_type_id=(
            interval_reference_ht.index(
                mt.locus,
                all_matches=True,
            ).flatmap(
                lambda x: x.screen['region_type_id'],
            )
        ),
    )
