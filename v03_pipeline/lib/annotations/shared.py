from typing import Any

import hail as hl

from v03_pipeline.lib.annotations import expression_helpers
from v03_pipeline.lib.annotations.enums import (
    BIOTYPES,
    MOTIF_CONSEQUENCE_TERMS,
    REGULATORY_CONSEQUENCE_TERMS,
)
from v03_pipeline.lib.annotations.vep import (
    add_transcript_rank,
)
from v03_pipeline.lib.model.definitions import ReferenceGenome

BIOTYPE_LOOKUP = hl.dict(hl.enumerate(BIOTYPES, index_first=False))
MOTIF_CONSEQUENCE_TERMS_LOOKUP = hl.dict(
    hl.enumerate(MOTIF_CONSEQUENCE_TERMS, index_first=False),
)
REGULATORY_CONSEQUENCE_TERMS_LOOKUP = hl.dict(
    hl.enumerate(REGULATORY_CONSEQUENCE_TERMS, index_first=False),
)


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


def check_ref(ht: hl.Table, **_: Any) -> hl.BooleanExpression:
    return hl.is_defined(ht.vep.check_ref)


def sorted_motif_feature_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    # NB: hl.min() doesn't work correctly in the "sorted" key expression, so we have an additional
    # is_defined check, even though the expression should work on missing on its own.
    # hail bug report incoming.
    result = hl.or_missing(
        hl.is_defined(ht.vep.motif_feature_consequences),
        hl.sorted(
            ht.vep.motif_feature_consequences.map(
                lambda c: c.select(
                    consequence_term_ids=c.consequence_terms.map(
                        lambda t: MOTIF_CONSEQUENCE_TERMS_LOOKUP[t],
                    ),
                    motif_feature_id=c.motif_feature_id,
                ),
            ).filter(lambda c: c.consequence_term_ids.size() > 0),
            lambda c: hl.min(c.consequence_term_ids),
        ),
    )
    return add_transcript_rank(result)


def sorted_regulatory_feature_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    result = hl.or_missing(
        hl.is_defined(ht.vep.regulatory_feature_consequences),
        hl.sorted(
            ht.vep.regulatory_feature_consequences.map(
                lambda c: c.select(
                    biotype_id=BIOTYPE_LOOKUP[c.biotype],
                    consequence_term_ids=c.consequence_terms.map(
                        lambda t: REGULATORY_CONSEQUENCE_TERMS_LOOKUP[t],
                    ),
                    regulatory_feature_id=c.regulatory_feature_id,
                ),
            ).filter(lambda c: c.consequence_term_ids.size() > 0),
            lambda c: hl.min(c.consequence_term_ids),
        ),
    )
    return add_transcript_rank(result)
