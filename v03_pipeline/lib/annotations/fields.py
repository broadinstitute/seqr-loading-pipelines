from __future__ import annotations

from typing import TYPE_CHECKING, Any

from v03_pipeline.lib.annotations import (
    gcnv,
    reference_dataset_collection,
    sample_lookup_table,
    shared,
    sv,
)
from v03_pipeline.lib.model import AnnotationType, DatasetType

if TYPE_CHECKING:
    import hail as hl


ANNOTATION_CONFIG = {
    (DatasetType.SNV, AnnotationType.FORMATTING): [
        shared.rg37_locus,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    (DatasetType.SNV, AnnotationType.REFERENCE_DATASET_COLLECTION): [
        reference_dataset_collection.hgmd,
        reference_dataset_collection.gnomad_non_coding_constraint,
        reference_dataset_collection.screen,
    ],
    (DatasetType.SNV, AnnotationType.SAMPLE_LOOKUP_TABLE): [
        sample_lookup_table.AC,
        sample_lookup_table.AF,
        sample_lookup_table.AN,
    ],
    (DatasetType.MITO, AnnotationType.FORMATTING): [
        shared.rg37_locus,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    (DatasetType.MITO, AnnotationType.SAMPLE_LOOKUP_TABLE): [
        sample_lookup_table.AC,
        sample_lookup_table.AF,
        sample_lookup_table.AN,
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


def get_fields(
    ht: hl.Table,
    annotation_type: AnnotationType,
    **kwargs: Any,
) -> dict[str, hl.Expression]:
    dataset_type = kwargs['dataset_type']
    fields = {
        field_expression.__name__: field_expression(ht, **kwargs)
        for field_expression in ANNOTATION_CONFIG.get((dataset_type, annotation_type), [])
    }
    return {k: v for k, v in fields.items() if v is not None}
