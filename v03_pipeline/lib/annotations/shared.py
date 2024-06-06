from typing import Any

import hail as hl

from v03_pipeline.lib.annotations import expression_helpers
from v03_pipeline.lib.annotations.vep import (
    transcript_consequences_sort,
    vep_85_transcript_consequences_select,
)
from v03_pipeline.lib.model.definitions import ReferenceGenome


def add_rg38_liftover(liftover_ref_path: str) -> None:
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(liftover_ref_path, rg37)


def GT(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    return mt.GT


def GQ(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.if_else(is_called, mt.GQ, 0)


def rsid(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return mt.rsid


def rg37_locus(
    ht: hl.Table,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    add_rg38_liftover(liftover_ref_path)
    return hl.liftover(ht.locus, ReferenceGenome.GRCh37.value)


def xpos(ht: hl.Table, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_xpos(ht.locus)


def variant_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_variant_id(ht)


def sorted_transcript_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.sorted(
        ht.vep.transcript_consequences.map(
            vep_85_transcript_consequences_select,
        ).filter(lambda c: c.consequence_term_ids.size() > 0),
        transcript_consequences_sort(ht),
    )
