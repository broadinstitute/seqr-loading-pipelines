from __future__ import annotations

from typing import Any

import hail as hl

from v03_pipeline.lib.annotations.enums import MITOTIP_PATHOGENICITIES

MITOTIP_PATHOGENICITIES_LOOKUP = hl.dict(
    hl.enumerate(MITOTIP_PATHOGENICITIES, index_first=False).extend(
        # NB: adding missing values here allows us to
        # hard fail if a mapped key is present and has an unexpected value
        # but propagate missing values.
        [(hl.missing(hl.tstr), hl.missing(hl.tint32))],
    ),
)


def _AC_het(row: hl.StructExpression) -> hl.Int32Expression:  # noqa: N802
    return sum(
        row.heteroplasmic_samples[project_guid].length()
        for project_guid in row.heteroplasmic_samples
    )


def _AC_hom(row: hl.StructExpression) -> hl.Int32Expression:  # noqa: N802
    return sum(
        row.homoplasmic_samples[project_guid].length()
        for project_guid in row.homoplasmic_samples
    )


def _AN(row: hl.StructExpression) -> hl.Int32Expression:  # noqa: N802
    return sum(
        (
            row.ref_samples[project_guid].length()
            + row.heteroplasmic_samples[project_guid].length()
            + row.homoplasmic_samples[project_guid].length()
        )
        for project_guid in row.ref_samples
    )


def common_low_heteroplasmy(ht: hl.Table, **_: Any) -> hl.Expression:
    return ht.common_low_heteroplasmy


def contamination(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return mt.contamination


def DP(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.cond(is_called, hl.int32(hl.min(mt.DP, 32000)), hl.missing(hl.tint32))


def GQ(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.if_else(is_called, mt.MQ, 0)


def haplogroup(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.Struct(
        is_defining=ht.hap_defining_variant,
    )


def HL(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.if_else(is_called, mt.HL, 0)


def high_constraint_region(
    ht: hl.Table,
    interval_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.is_defined(interval_ht[ht.locus])


def mito_cn(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return hl.int(mt.mito_cn)


def mitotip(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.Struct(
        trna_prediction_id=MITOTIP_PATHOGENICITIES_LOOKUP[ht.mitotip_trna_prediction],
    )


def rsid(ht: hl.Table, **_: Any) -> hl.Expression:
    return ht.rsid.find(lambda x: hl.is_defined(x))


def gt_stats(ht: hl.Table, sample_lookup_ht: hl.Table, **_: Any) -> hl.Expression:
    row = sample_lookup_ht[ht.key]
    return hl.Struct(
        AC_het=_AC_het(row),
        AF_het=hl.float32(_AC_het(row) / _AN(row)),
        AC_hom=_AC_hom(row),
        AF_hom=hl.float32(_AC_hom(row) / _AN(row)),
        AN=_AN(row),
    )
