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


def high_constraint_region_mito(
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


def gt_stats(
    ht: hl.Table,
    lookup_ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    row = lookup_ht[ht.key]
    ref_samples = hl.sum(hl.flatten(row.project_stats.ref_samples))
    heteroplasmic_samples = hl.sum(hl.flatten(row.project_stats.heteroplasmic_samples))
    homoplasmic_samples = hl.sum(hl.flatten(row.project_stats.homoplasmic_samples))
    AC_het = heteroplasmic_samples  # noqa: N806
    AC_hom = homoplasmic_samples  # noqa: N806
    AN = (  # noqa: N806
        ref_samples + heteroplasmic_samples + homoplasmic_samples
    )
    return hl.Struct(
        AC_het=AC_het,
        AF_het=hl.float32(AC_het / AN),
        AC_hom=AC_hom,
        AF_hom=hl.float32(AC_hom / AN),
        AN=AN,
    )
