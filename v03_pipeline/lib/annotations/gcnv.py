from __future__ import annotations

from typing import Any

import hail as hl

from hail_scripts.computed_fields import variant_id as expression_helpers

from v03_pipeline.lib.annotations.enums import SV_CONSEQUENCE_RANKS, SV_TYPES
from v03_pipeline.lib.annotations.shared import add_rg38_liftover
from v03_pipeline.lib.misc.gcnv import parse_gcnv_genes
from v03_pipeline.lib.model.definitions import ReferenceGenome

SV_CONSEQUENCE_RANKS_LOOKUP = hl.dict(
    hl.enumerate(SV_CONSEQUENCE_RANKS, index_first=False),
)
SV_TYPES_LOOKUP = hl.dict(hl.enumerate(SV_TYPES, index_first=False))


def _start_and_end_equal(mt: hl.MatrixTable) -> hl.BooleanExpression:
    return (mt.sample_start == mt.start) & (mt.sample_end == mt.end)


def CN(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    return mt.CN


def defragged(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return mt.defragmented


def end_locus(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    **_: Any,
) -> hl.LocusExpression:
    return hl.locus(ht.chr, ht.end, reference_genome.value)


def gt_stats(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(
        AF=ht.sf,
        AC=ht.sc,
        AN=hl.int32(ht.sc / ht.sf),
        Hom=hl.missing(hl.tint32),
        Het=hl.missing(hl.tint32),
    )


def GT(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    return hl.if_else(
        (mt.CN == 0) | (mt.CN > 3),  # noqa: PLR2004
        hl.Call([1, 1], phased=False),
        hl.Call([0, 1], phased=False),
    )


def new_call(
    mt: hl.MatrixTable,
    is_new_gcnv_joint_call: bool,
    **_: Any,
) -> hl.Expression:
    if is_new_gcnv_joint_call:
        return mt.no_ovl
    return False


def num_exon(ht: hl.Table, **_: Any) -> hl.Expression:
    return ht.num_exon


def prev_call(
    mt: hl.MatrixTable,
    is_new_gcnv_joint_call: bool,
    **_: Any,
) -> hl.Expression:
    if is_new_gcnv_joint_call:
        return hl.len(mt.identical_ovl) > 0
    return ~mt.is_latest


def prev_overlap(
    mt: hl.MatrixTable,
    is_new_gcnv_joint_call: bool,
    **_: Any,
) -> hl.Expression:
    if is_new_gcnv_joint_call:
        return hl.len(mt.any_ovl) > 0
    return False


def QS(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    return mt.QS


def rg37_locus(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    if reference_genome == ReferenceGenome.GRCh37:
        return None
    add_rg38_liftover(liftover_ref_path)
    return hl.liftover(start_locus(ht, reference_genome), ReferenceGenome.GRCh37.value)


def rg37_locus_end(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    if reference_genome == ReferenceGenome.GRCh37:
        return None
    add_rg38_liftover(liftover_ref_path)
    return hl.liftover(end_locus(ht, reference_genome), ReferenceGenome.GRCh37.value)


def sample_end(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return hl.or_missing(
        ~_start_and_end_equal(mt),
        mt.sample_end,
    )


def sample_gene_ids(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    parsed_genes = parse_gcnv_genes(mt.genes_any_overlap_Ensemble_ID)
    return hl.or_missing(parsed_genes != mt.gene_ids, parsed_genes)


def sample_start(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return hl.or_missing(
        ~_start_and_end_equal(mt),
        mt.sample_start,
    )


def sample_num_exon(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return hl.or_missing(
        mt.genes_any_overlap_totalExons != mt.num_exon,
        mt.genes_any_overlap_totalExons,
    )


def sorted_gene_consequences(
    ht: hl.Table,
    **_: Any,
) -> hl.Expression:
    return hl.array(
        ht.gene_ids.map(
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


def start_locus(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    **_: Any,
) -> hl.LocusExpression:
    return hl.locus(ht.chr, ht.start, reference_genome.value)


def strvctvre(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(score=hl.parse_float(ht.strvctvre_score))


def sv_type_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return SV_TYPES_LOOKUP[ht.svtype]


def xpos(ht: hl.Table, reference_genome: ReferenceGenome, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_xpos(start_locus(ht, reference_genome))
