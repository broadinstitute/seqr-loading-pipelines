from __future__ import annotations

from enum import Enum
from typing import Any, Callable

import hail as hl

from hail_scripts.computed_fields.vep import (
    CONSEQUENCE_TERM_RANK_LOOKUP,
    get_expr_for_vep_sorted_transcript_consequences_array,
    get_expr_for_worst_transcript_consequence_annotations_struct,
)
from hail_scripts.reference_data.clinvar import CLINVAR_PATHOGENICITIES_LOOKUP

from v03_pipeline.lib.model.definitions import AccessControl, ReferenceGenome

CLINVAR_PATH_RANGE = ('Pathogenic', 'Pathogenic/Likely_risk_allele')
CLINVAR_LIKELY_PATH_RANGE = ('Pathogenic/Likely_pathogenic', 'Likely_risk_allele')
GNOMAD_CODING_NONCODING_HIGH_AF_THRESHOLD = 0.90
ONE_PERCENT = 0.01
TEN_PERCENT = 0.10


def clinvar_path_variants(
    ht: hl.Table,
    **_: Any,
) -> hl.Table:
    ht = ht.select_globals()
    ht = ht.select(
        pathogenic=(
            (
                ht.clinvar.pathogenicity_id
                >= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_PATH_RANGE[0]]
            )
            & (
                ht.clinvar.pathogenicity_id
                < CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_PATH_RANGE[1]]
            )
        ),
        likely_pathogenic=(
            (
                ht.clinvar.pathogenicity_id
                >= CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_LIKELY_PATH_RANGE[0]]
            )
            & (
                ht.clinvar.pathogenicity_id
                < CLINVAR_PATHOGENICITIES_LOOKUP[CLINVAR_LIKELY_PATH_RANGE[1]]
            )
        ),
    )
    ht = ht.filter(ht.pathogenic | ht.likely_pathogenic)
    return ht


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
            <= CONSEQUENCE_TERM_RANK_LOOKUP['synonymous_variant']
        ),
        noncoding=(
            ht.main_transcript.major_consequence_rank
            >= CONSEQUENCE_TERM_RANK_LOOKUP['downstream_gene_variant']
        ),
    )
    return ht.filter(ht.coding | ht.noncoding)


def high_af_variants(
    ht: hl.Table,
    **_: Any,
) -> hl.Table:
    ht = ht.select_globals()
    ht = ht.filter(ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL > ONE_PERCENT)
    ht = ht.select(is_gt_10_percent=ht.gnomad_genomes.AF_POPMAX_OR_GLOBAL > TEN_PERCENT)
    return ht


def gnomad_qc(
    ht: hl.Table,
    **_: Any,
) -> hl.Table:
    return ht.select()


class CachedReferenceDatasetQuery(Enum):
    CLINVAR_PATH_VARIANTS = 'clinvar_path_variants'
    GNOMAD_CODING_AND_NONCODING_VARIANTS = 'gnomad_coding_and_noncoding_variants'
    GNOMAD_QC = 'gnomad_qc'
    HIGH_AF_VARIANTS = 'high_af_variants'

    @property
    def access_control(self) -> AccessControl:
        if self == CachedReferenceDatasetQuery.GNOMAD_QC:
            return AccessControl.PRIVATE
        return AccessControl.PUBLIC

    @property
    def reference_dataset(self) -> str | None:
        return {
            CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS: 'gnomad_genomes',
            CachedReferenceDatasetQuery.GNOMAD_QC: 'gnomad_qc',
        }.get(self)

    @property
    def query(self) -> Callable[[hl.Table, ReferenceGenome], hl.Table]:
        return {
            CachedReferenceDatasetQuery.CLINVAR_PATH_VARIANTS: clinvar_path_variants,
            CachedReferenceDatasetQuery.GNOMAD_CODING_AND_NONCODING_VARIANTS: gnomad_coding_and_noncoding_variants,
            CachedReferenceDatasetQuery.GNOMAD_QC: gnomad_qc,
            CachedReferenceDatasetQuery.HIGH_AF_VARIANTS: high_af_variants,
        }[self]
