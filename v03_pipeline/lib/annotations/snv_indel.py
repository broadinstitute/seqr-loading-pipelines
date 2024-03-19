# ruff: noqa: N806
from typing import Any

import hail as hl

N_ALT_REF = 0
N_ALT_HET = 1
N_ALT_HOM = 2


def AB(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.bind(
        lambda total: hl.if_else(
            (is_called) & (total != 0) & (hl.len(mt.AD) > 1),
            hl.float32(mt.AD[1] / total),
            hl.missing(hl.tfloat32),
        ),
        hl.sum(mt.AD),
    )


def DP(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.if_else(
        is_called & hl.is_defined(mt.AD),
        hl.int32(hl.min(hl.sum(mt.AD), 32000)),
        hl.missing(hl.tint32),
    )


def gt_stats(
    ht: hl.Table,
    lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    row = lookup_ht[ht.key]
    ref_samples = hl.sum(hl.flatten(row.project_stats.ref_samples))
    het_samples = hl.sum(hl.flatten(row.project_stats.het_samples))
    hom_samples = hl.sum(hl.flatten(row.project_stats.hom_samples))
    AC = ref_samples * N_ALT_REF + het_samples * N_ALT_HET + hom_samples * N_ALT_HOM
    AN = 2 * (ref_samples + het_samples + hom_samples)
    hom = hom_samples
    return hl.Struct(
        AC=AC,
        AN=AN,
        AF=hl.float32(AC / AN),
        hom=hom,
    )


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
