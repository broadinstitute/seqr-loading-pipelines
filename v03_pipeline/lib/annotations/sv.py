from __future__ import annotations

from typing import Any

import hail as hl

from v03_pipeline.lib.annotations.enums import (
    SV_CONSEQUENCE_RANKS,
    SV_TYPE_DETAILS,
    SV_TYPES,
)
from v03_pipeline.lib.annotations.shared import add_rg38_liftover
from v03_pipeline.lib.model.definitions import ReferenceGenome

CONSEQ_PREDICTED_PREFIX = 'PREDICTED_'
NON_GENE_PREDICTIONS = {
    'PREDICTED_INTERGENIC',
    'PREDICTED_NONCODING_BREAKPOINT',
    'PREDICTED_NONCODING_SPAN',
}

PREVIOUS_GENOTYPE_N_ALT_ALLELES = hl.dict(
    {
        # Map of concordance string -> previous n_alt_alleles()
        # Concordant
        frozenset(['TN']): 0,  # 0/0 -> 0/0
        frozenset(['TP']): 2,  # 1/1 -> 1/1
        frozenset(['TN', 'TP']): 1,  # 0/1 -> 0/1
        # Novel
        frozenset(['FP']): 0,  # 0/0 -> 1/1
        frozenset(['TN', 'FP']): 0,  # 0/0 -> 0/1
        # Absent
        frozenset(['FN']): 2,  # 1/1 -> 0/0
        frozenset(['TN', 'FN']): 1,  # 0/1 -> 0/0
        # Discordant
        frozenset(['FP', 'TP']): 1,  # 0/1 -> 1/1
        frozenset(['FN', 'TP']): 2,  # 1/1 -> 0/1
    },
)


SV_TYPES_LOOKUP = hl.dict(hl.enumerate(SV_TYPES, index_first=False))
SV_TYPE_DETAILS_LOOKUP = hl.dict(hl.enumerate(SV_TYPE_DETAILS, index_first=False))
SV_CONSEQUENCE_RANKS_LOOKUP = hl.dict(
    hl.enumerate(SV_CONSEQUENCE_RANKS, index_first=False),
)


def _get_cpx_interval(x):
    # an example format of CPX_INTERVALS is "DUP_chr1:1499897-1499974"
    type_chr = x.split('_chr')
    chr_pos = type_chr[1].split(':')
    pos = chr_pos[1].split('-')
    return hl.struct(
        type=type_chr[0],
        chrom=chr_pos[0],
        start=hl.int32(pos[0]),
        end=hl.int32(pos[1]),
    )


def _sv_types(ht: hl.Table) -> hl.ArrayExpression:
    return ht.alleles[1].replace('[<>]', '').split(':', 2)


def algorithms(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.str(',').join(ht.info.ALGORITHMS)


def bothsides_support(ht: hl.Table, **_: Any) -> hl.Expression:
    return ht.filters.any(lambda x: x == 'BOTHSIDES_SUPPORT')


def CN(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    return mt.RD_CN


def concordance(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    is_called = hl.is_defined(mt.GT)
    was_previously_called = hl.is_defined(mt.CONC_ST) & ~mt.CONC_ST.contains(
        'EMPTY',
    )
    num_alt = hl.if_else(is_called, mt.GT.n_alt_alleles(), -1)
    prev_num_alt = hl.if_else(
        was_previously_called,
        PREVIOUS_GENOTYPE_N_ALT_ALLELES[hl.set(mt.CONC_ST)],
        -1,
    )
    concordant_genotype = num_alt == prev_num_alt
    discordant_genotype = (num_alt != prev_num_alt) & (prev_num_alt > 0)
    novel_genotype = (num_alt != prev_num_alt) & (prev_num_alt == 0)
    return hl.struct(
        prev_num_alt=hl.or_missing(discordant_genotype, prev_num_alt),
        prev_call=hl.or_missing(is_called, was_previously_called & concordant_genotype),
        new_call=hl.or_missing(is_called, ~was_previously_called | novel_genotype),
    )


def cpx_intervals(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.or_missing(
        hl.is_defined(ht.info.CPX_INTERVALS),
        ht.info.CPX_INTERVALS.map(lambda x: _get_cpx_interval(x)),
    )


def end_locus(ht: hl.Table, **_: Any) -> hl.StructExpression:
    rg38_lengths = hl.literal(hl.get_reference(ReferenceGenome.GRCh38.value).lengths)
    return hl.if_else(
        (hl.is_defined(ht.info.END2) & (ht.info.END2 <= rg38_lengths[ht.info.CHR2])),
        hl.locus(ht.info.CHR2, ht.info.END2, ReferenceGenome.GRCh38.value),
        hl.or_missing(
            (ht.info.END <= rg38_lengths[ht.locus.contig]),
            hl.locus(ht.locus.contig, ht.info.END, ReferenceGenome.GRCh38.value),
        ),
    )


def gnomad_svs(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.or_missing(
        hl.is_defined(ht.info.gnomAD_V2_AF),
        hl.struct(AF=ht.info.gnomAD_V2_AF, ID=ht.info.gnomAD_V2_SVID),
    )


def gt_stats(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(
        AF=ht.info.AF[0],
        AC=ht.info.AC[0],
        AN=ht.info.AN,
        Hom=ht.info.N_HOMALT,
        Het=ht.info.N_HET,
    )


def rg37_locus_end(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    if reference_genome == ReferenceGenome.GRCh37:
        return None
    add_rg38_liftover(liftover_ref_path)
    end = end_locus(ht)
    return hl.or_missing(
        hl.is_defined(end),
        hl.liftover(
            hl.locus(
                end.contig,
                end.position,
                reference_genome=ReferenceGenome.GRCh38.value,
            ),
            ReferenceGenome.GRCh37.value,
        ),
    )


def start_locus(ht: hl.Table, **_: Any):
    return ht.locus


def sorted_gene_consequences(
    ht: hl.Table,
    gencode_mapping: dict[str, str],
    **_: Any,
) -> hl.Expression:
    # In lieu of sorted_transcript_consequences seen on SNV/MITO.
    conseq_predicted_gene_cols = [
        gene_col
        for gene_col in ht.info
        if gene_col.startswith(CONSEQ_PREDICTED_PREFIX)
        and gene_col not in NON_GENE_PREDICTIONS
    ]
    mapped_genes = [
        ht.info[gene_col].map(
            lambda gene: hl.struct(
                gene_id=gencode_mapping.get(gene),
                major_consequence_id=SV_CONSEQUENCE_RANKS_LOOKUP[
                    gene_col.replace(CONSEQ_PREDICTED_PREFIX, '', 1)  # noqa: B023
                ],
            ),
        )
        for gene_col in conseq_predicted_gene_cols
    ]
    return hl.filter(hl.is_defined, mapped_genes).flatmap(lambda x: x)


def strvctvre(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.struct(score=hl.parse_float(ht.info.StrVCTVRE))


def sv_type_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return SV_TYPES_LOOKUP[_sv_types(ht)[0]]


def sv_type_detail_id(ht: hl.Table, **_: Any) -> hl.Expression:
    sv_types = _sv_types(ht)
    return hl.if_else(
        sv_types[0] == 'CPX',
        SV_TYPE_DETAILS_LOOKUP[ht.info.CPX_TYPE],
        hl.or_missing(
            (sv_types[0] == 'INS') & (hl.len(sv_types) > 1),
            SV_TYPE_DETAILS_LOOKUP[sv_types[1]],
        ),
    )
