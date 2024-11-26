from typing import Any

import hail as hl

from v03_pipeline.lib.annotations.enums import (
    CLINVAR_PATHOGENICITIES_LOOKUP,
    TRANSCRIPT_CONSEQUENCE_TERMS,
)
from v03_pipeline.lib.annotations.expression_helpers import (
    get_expr_for_vep_sorted_transcript_consequences_array,
    get_expr_for_worst_transcript_consequence_annotations_struct,
)
from v03_pipeline.lib.model.dataset_type import DatasetType
from v03_pipeline.lib.model.definitions import ReferenceGenome

CLINVAR_PATH_RANGE = ('Pathogenic', 'Pathogenic/Likely_risk_allele')
CLINVAR_LIKELY_PATH_RANGE = ('Pathogenic/Likely_pathogenic', 'Likely_risk_allele')
TRANSCRIPT_CONSEQUENCE_TERM_RANK_LOOKUP = hl.dict(
    hl.enumerate(TRANSCRIPT_CONSEQUENCE_TERMS, index_first=False),
)
GNOMAD_CODING_NONCODING_HIGH_AF_THRESHOLD = 0.90
ONE_TENTH_PERCENT = 0.001
ONE_PERCENT = 0.01
THREE_PERCENT = 0.03
FIVE_PERCENT = 0.05
TEN_PERCENT = 0.10


def clinvar_path_variants(
    ht: hl.Table,
    dataset_type: DatasetType,
    **_: Any,
) -> hl.Table:
    clinvar_field = 'clinvar_mito' if dataset_type == DatasetType.MITO else 'clinvar'
    ht = ht.select_globals()
    ht = ht.select(
        is_pathogenic=(
            (
                ht[clinvar_field].pathogenicity_id
                >= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_PATH_RANGE[0]]
            )
            & (
                ht[clinvar_field].pathogenicity_id
                <= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_PATH_RANGE[1]]
            )
        ),
        is_likely_pathogenic=(
            (
                ht[clinvar_field].pathogenicity_id
                >= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_LIKELY_PATH_RANGE[0]]
            )
            & (
                ht[clinvar_field].pathogenicity_id
                <= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_LIKELY_PATH_RANGE[1]]
            )
        ),
    )
    return ht.filter(ht.is_pathogenic | ht.is_likely_pathogenic)


def gnomad_coding_and_noncoding_variants(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    **_: Any,
) -> hl.Table:
    filtered_contig = 'chr1' if reference_genome == ReferenceGenome.GRCh38 else '1'
    ht = hl.filter_intervals(
        ht,
        [
            hl.parse_locus_interval(
                filtered_contig,
                reference_genome=reference_genome.value,
            ),
        ],
    )
    ht = ht.filter(ht.freq[0].AF > GNOMAD_CODING_NONCODING_HIGH_AF_THRESHOLD)
    ht = ht.annotate(
        sorted_transaction_consequences=(
            get_expr_for_vep_sorted_transcript_consequences_array(
                ht.vep,
                omit_consequences=[],
            )
        ),
    )
    ht = ht.annotate(
        main_transcript=(
            get_expr_for_worst_transcript_consequence_annotations_struct(
                ht.sorted_transaction_consequences,
            )
        ),
    )
    ht = ht.select(
        coding=(
            ht.main_transcript.major_consequence_rank
            <= TRANSCRIPT_CONSEQUENCE_TERM_RANK_LOOKUP['synonymous_variant']
        ),
        noncoding=(
            ht.main_transcript.major_consequence_rank
            >= TRANSCRIPT_CONSEQUENCE_TERM_RANK_LOOKUP['downstream_gene_variant']
        ),
    )
    return ht.filter(ht.coding | ht.noncoding)


def high_af_variants(
    ht: hl.Table,
    **_: Any,
) -> hl.Table:
    ht = ht.select_globals()
    ht = ht.filter(ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL > ONE_TENTH_PERCENT)
    return ht.select(
        is_gt_1_percent=ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL > ONE_PERCENT,
        is_gt_3_percent=ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL > THREE_PERCENT,
        is_gt_5_percent=ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL > FIVE_PERCENT,
        is_gt_10_percent=ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL > TEN_PERCENT,
    )


def gnomad_qc(
    ht: hl.Table,
    **_: Any,
) -> hl.Table:
    return ht.select()
