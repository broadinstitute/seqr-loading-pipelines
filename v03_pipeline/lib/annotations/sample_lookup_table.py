from __future__ import annotations

from typing import Any

import hail as hl

N_ALT_REF = 0
N_ALT_HET = 1
N_ALT_HOM = 2


def _AC(row: hl.StructExpression) -> hl.Int32Expression:  # noqa: N802
    return sum(
        [
            (
                row.ref_samples[project_guid].length() * N_ALT_REF
                + row.het_samples[project_guid].length() * N_ALT_HET
                + row.hom_samples[project_guid].length() * N_ALT_HOM
            )
            for project_guid in row.ref_samples
        ],
    )


def _AN(row: hl.StructExpression) -> hl.Int32Expression:  # noqa: N802
    return 2 * sum(
        [
            (
                row.ref_samples[project_guid].length()
                + row.het_samples[project_guid].length()
                + row.hom_samples[project_guid].length()
            )
            for project_guid in row.ref_samples
        ],
    )


def _AF(row: hl.StructExpression) -> hl.Float32Expression:  # noqa: N802
    return hl.float32(_AC(row) / _AN(row))


def _hom(row: hl.StructExpression) -> hl.Int32Expression:
    return sum(
        [row.hom_samples[project_guid].length() for project_guid in row.hom_samples],
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
