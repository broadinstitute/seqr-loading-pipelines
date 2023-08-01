from __future__ import annotations

from typing import Any

import hail as hl

from hail_scripts.computed_fields import variant_id as expression_helpers

from v03_pipeline.lib.annotations.enums import SV_CONSEQUENCE_RANKS, SV_TYPES
from v03_pipeline.lib.annotations.shared import add_rg38_liftover
from v03_pipeline.lib.model.definitions import ReferenceGenome

SV_CONSEQUENCE_RANKS_LOOKUP = hl.dict(
    hl.enumerate(SV_CONSEQUENCE_RANKS, index_first=False),
)
SV_TYPES_LOOKUP = hl.dict(hl.enumerate(SV_TYPES, index_first=False))


def _start_locus(ht: hl.Table, reference_genome: ReferenceGenome) -> hl.LocusExpression:
    return hl.locus(ht.chr, ht.start, reference_genome.value)


def _end_locus(ht: hl.Table, reference_genome: ReferenceGenome) -> hl.LocusExpression:
    return hl.locus(ht.chr, ht.end, reference_genome.value)


def gt_stats(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(
        AF=ht.sf,
        AC=ht.sc,
        AN=hl.int32(ht.sc / ht.sf),
        Hom=hl.missing(hl.tint32),
        Het=hl.missing(hl.tint32),
    )


def interval(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    **_: Any,
) -> hl.Expression:
    return hl.interval(
        _start_locus(ht, reference_genome),
        _end_locus(ht, reference_genome),
    )


def num_exon(ht: hl.Table, **_: Any) -> hl.Expression:
    return ht.num_exon


def rg37_locus(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    if reference_genome == ReferenceGenome.GRCh37:
        return None
    add_rg38_liftover(liftover_ref_path)
    return hl.liftover(_start_locus(ht, reference_genome), ReferenceGenome.GRCh37.value)


def rg37_locus_end(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    if reference_genome == ReferenceGenome.GRCh37:
        return None
    add_rg38_liftover(liftover_ref_path)
    return hl.liftover(_end_locus(ht, reference_genome), ReferenceGenome.GRCh37.value)


def sorted_gene_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.array(
        ht.genes.map(
            lambda gene: hl.Struct(
                gene_id=gene,
                major_consequence_id=hl.if_else(
                    ht.cg_genes.contains(gene),
                    SV_CONSEQUENCE_RANKS_LOOKUP['COPY_GAIN'],
                    hl.or_missing(
                        ht.lof_genes.contains(gene),
                        SV_CONSEQUENCE_RANKS_LOOKUP['LOF'],
                    ),
                ),
            ),
        ),
    )


def strvctvre(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(score=hl.parse_float(ht.strvctvre_score))


def sv_type_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return SV_TYPES_LOOKUP[ht.svtype]


def xpos(ht: hl.Table, reference_genome: ReferenceGenome, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_xpos(_start_locus(ht, reference_genome))
