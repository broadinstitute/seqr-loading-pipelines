from v03_pipeline.lib.annotations import (
    gcnv,
    mito,
    sample_lookup_table,
    shared,
    snv,
    sv,
)
from v03_pipeline.lib.model import AnnotationType, DatasetType

CONFIG = {
    (DatasetType.SNV, AnnotationType.FORMATTING): [
        shared.rg37_locus,
        shared.rsid,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    (DatasetType.SNV, AnnotationType.REFERENCE_DATASET_COLLECTION): [
        snv.gnomad_non_coding_constraint,
        snv.screen,
    ],
    (DatasetType.SNV, AnnotationType.SAMPLE_LOOKUP_TABLE): [
        sample_lookup_table.gt_stats,
    ],
    (DatasetType.SNV, AnnotationType.GENOTYPE_ENTRIES): [
        snv.GQ,
        snv.AB,
        snv.DP,
        shared.GT,
    ],
    (DatasetType.MITO, AnnotationType.FORMATTING): [
        mito.common_low_heteroplasmy,
        mito.callset_heteroplasmy,
        mito.haplogroup,
        mito.mitotip,
        shared.rg37_locus,
        mito.rsid,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    (DatasetType.MITO, AnnotationType.SAMPLE_LOOKUP_TABLE): [
        sample_lookup_table.gt_stats,
    ],
    (DatasetType.MITO, AnnotationType.REFERENCE_DATASET_COLLECTION): [
        mito.high_constraint_region,
    ],
    (DatasetType.SV, AnnotationType.FORMATTING): [
        shared.rg37_locus,
        sv.variant_id,
        shared.xpos,
    ],
    (DatasetType.GCNV, AnnotationType.FORMATTING): [
        gcnv.variant_id,
        gcnv.xpos,
    ],
}
