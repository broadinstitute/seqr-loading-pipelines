from __future__ import annotations

from typing import Any

import hail as hl

from hail_scripts.computed_fields import variant_id as expression_helpers

from v03_pipeline.lib.annotations.enums import BIOTYPES, CONSEQUENCE_TERMS, LOF_FILTERS
from v03_pipeline.lib.model.definitions import ReferenceGenome

BIOTYPE_LOOKUP = hl.dict(hl.enumerate(BIOTYPES, index_first=False))
CONSEQUENCE_TERMS_LOOKUP = hl.dict(hl.enumerate(CONSEQUENCE_TERMS, index_first=False))
LOF_FILTERS_LOOKUP = hl.dict(hl.enumerate(LOF_FILTERS, index_first=False))

PROTEIN_CODING_ID = BIOTYPE_LOOKUP['protein_coding']

OMIT_CONSEQUENCE_TERMS = hl.set(
    [
        'upstream_gene_variant',
        'downstream_gene_variant',
    ],
)

SELECTED_ANNOTATIONS = [
    'amino_acids',
    'canonical',
    'codons',
    'gene_id',
    'hgvsc',
    'hgvsp',
    'transcript_id',
]


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
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    if reference_genome == ReferenceGenome.GRCh37:
        return None
    add_rg38_liftover(liftover_ref_path)
    return hl.liftover(ht.locus, ReferenceGenome.GRCh37.value)


def xpos(ht: hl.Table, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_xpos(ht.locus)


def variant_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_variant_id(ht)


def sorted_transcript_consequences(ht: hl.Table, **_: Any) -> hl.Expression:
    result = hl.sorted(
        ht.vep.transcript_consequences.map(
            lambda c: c.select(
                *SELECTED_ANNOTATIONS,
                biotype_id=BIOTYPE_LOOKUP[c.biotype],
                consequence_term_ids=(
                    c.consequence_terms.filter(
                        lambda t: ~OMIT_CONSEQUENCE_TERMS.contains(t),
                    ).map(lambda t: CONSEQUENCE_TERMS_LOOKUP[t])
                ),
                is_lof_nagnag=c.lof_flags == 'NAGNAG_SITE',
                lof_filter_ids=hl.or_missing(
                    (c.lof == 'LC') & hl.is_defined(c.lof_filter),
                    c.lof_filter.split('&|,').map(lambda f: LOF_FILTERS_LOOKUP[f]),
                ),
            ),
        ).filter(lambda c: c.consequence_term_ids.size() > 0),
        lambda c: (
            hl.bind(
                lambda is_coding, is_most_severe, is_canonical: (
                    hl.cond(
                        is_coding,
                        hl.cond(
                            is_most_severe,
                            hl.cond(is_canonical, 1, 2),
                            hl.cond(is_canonical, 3, 4),
                        ),
                        hl.cond(
                            is_most_severe,
                            hl.cond(is_canonical, 5, 6),
                            hl.cond(is_canonical, 7, 8),
                        ),
                    )
                ),
                c.biotype_id == PROTEIN_CODING_ID,
                hl.set(c.consequence_term_ids).contains(
                    CONSEQUENCE_TERMS_LOOKUP[ht.vep.most_severe_consequence],
                ),
                hl.or_else(c.canonical, 0) == 1,
            )
        ),
    )
    return hl.zip_with_index(result).map(
        lambda csq_with_index: csq_with_index[1].annotate(
            transcript_rank=csq_with_index[0],
        ),
    )


def strvctvre(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(score=hl.parse_float(ht.info.StrVCTVRE))
