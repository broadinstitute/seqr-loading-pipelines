from __future__ import annotations

from typing import Any

import hail as hl

from v03_pipeline.lib.annotations import (
    gcnv,
    reference_dataset_collection,
    sample_lookup_table,
    shared,
    snv,
    sv,
)
from v03_pipeline.lib.model import AnnotationType, DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.paths import (
    sample_lookup_table_path,
    valid_reference_dataset_collection_path,
)

ANNOTATION_CONFIG = {
    (DatasetType.SNV, AnnotationType.FORMATTING): [
        shared.rg37_locus,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    (DatasetType.SNV, AnnotationType.REFERENCE_DATASET_COLLECTION): [
        reference_dataset_collection.gnomad_non_coding_constraint,
        reference_dataset_collection.screen,
    ],
    (DatasetType.SNV, AnnotationType.SAMPLE_LOOKUP_TABLE): [
        sample_lookup_table.gt_stats,
    ],
    (DatasetType.SNV, AnnotationType.GENOTYPE_ENTRIES): [
        snv.gq,
        snv.ab,
        snv.dp,
        shared.gt,
        shared.sample_id,
    ],
    (DatasetType.MITO, AnnotationType.FORMATTING): [
        shared.rg37_locus,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    (DatasetType.MITO, AnnotationType.SAMPLE_LOOKUP_TABLE): [
        sample_lookup_table.gt_stats,
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


def hail_table_dependencies(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    annotation_type: AnnotationType,
) -> dict[str, hl.Table]:
    if annotation_type == AnnotationType.REFERENCE_DATASET_COLLECTION:
        return {
            f'{rdc.value}_ht': hl.read_table(
                valid_reference_dataset_collection_path(
                    env,
                    reference_genome,
                    rdc,
                ),
            )
            for rdc in dataset_type.annotatable_reference_dataset_collections
        }
    if annotation_type == AnnotationType.SAMPLE_LOOKUP_TABLE:
        return {
            'sample_lookup_ht': hl.read_table(
                sample_lookup_table_path(
                    env,
                    reference_genome,
                    dataset_type,
                ),
            ),
        }
    return {}


def get_fields(
    t: hl.Table | hl.MatrixTable,
    annotation_type: AnnotationType,
    **kwargs: Any,
) -> dict[str, hl.Expression]:
    dataset_type = kwargs['dataset_type']
    hts = hail_table_dependencies(
        kwargs['env'],
        kwargs['reference_genome'],
        dataset_type,
        annotation_type,
    )
    fields = {
        field_expression.__name__: field_expression(t, **kwargs, **hts)
        for field_expression in ANNOTATION_CONFIG.get(
            (dataset_type, annotation_type),
            [],
        )
    }
    return {k: v for k, v in fields.items() if v is not None}
