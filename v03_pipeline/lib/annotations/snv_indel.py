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
            hl.float(mt.AD[1] / total),
            hl.missing(hl.tfloat),
        ),
        hl.sum(mt.AD),
    )


def DP(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.if_else(
        is_called & hl.is_defined(mt.AD),
        hl.int(hl.min(hl.sum(mt.AD), 32000)),
        hl.missing(hl.tint),
    )


def gt_stats(
    ht: hl.Table,
    sample_lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    row = sample_lookup_ht[ht.key]
    AC, AN, hom = 0, 0, 0
    for project_guid in row.ref_samples:
        ref_samples_length = row.ref_samples[project_guid].length()
        het_samples_length = row.het_samples[project_guid].length()
        hom_samples_length = row.hom_samples[project_guid].length()
        AC += (
            ref_samples_length * N_ALT_REF
            + het_samples_length * N_ALT_HET
            + hom_samples_length * N_ALT_HOM
        )
        AN += 2 * (ref_samples_length + het_samples_length + hom_samples_length)
        hom += hom_samples_length
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
