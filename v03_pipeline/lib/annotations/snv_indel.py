from typing import Any

import hail as hl

N_ALT_REF = 0
N_ALT_HET = 1
N_ALT_HOM = 2


def _AC(row: hl.StructExpression) -> hl.Int32Expression:  # noqa: N802
    return sum(
        (
            row.ref_samples[project_guid].length() * N_ALT_REF
            + row.het_samples[project_guid].length() * N_ALT_HET
            + row.hom_samples[project_guid].length() * N_ALT_HOM
        )
        for project_guid in row.ref_samples
    )


def _AF(row: hl.StructExpression) -> hl.Float32Expression:  # noqa: N802
    return hl.float32(_AC(row) / _AN(row))


def _AN(row: hl.StructExpression) -> hl.Int32Expression:  # noqa: N802
    return 2 * sum(
        (
            row.ref_samples[project_guid].length()
            + row.het_samples[project_guid].length()
            + row.hom_samples[project_guid].length()
        )
        for project_guid in row.ref_samples
    )


def _hom(row: hl.StructExpression) -> hl.Int32Expression:
    return sum(
        row.hom_samples[project_guid].length() for project_guid in row.hom_samples
    )


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
    return hl.Struct(
        AC=_AC(row),
        AN=_AN(row),
        AF=_AF(row),
        hom=_hom(row),
    )


def gnomad_non_coding_constraint(
    ht: hl.Table,
    interval_ht: hl.Table | None = None,
    **_: Any,
) -> hl.Expression:
    if not interval_ht:
        return None
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
    interval_ht: hl.Table | None = None,
    **_: Any,
) -> hl.Expression:
    if not interval_ht:
        return None
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
