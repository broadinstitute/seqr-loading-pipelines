from __future__ import annotations

from typing import TYPE_CHECKING, Any

from v03_pipeline.lib.annotations import gcnv, reference_dataset_collection, shared, sv
from v03_pipeline.lib.model import DatasetType, ReferenceDatasetCollection

if TYPE_CHECKING:
    import hail as hl

REFERENCE_DATASET_COLLECTION_ANNOTATIONS = {
    ReferenceDatasetCollection.HGMD: [
        reference_dataset_collection.hgmd,
    ],
    ReferenceDatasetCollection.INTERVAL_REFERENCE: [
        reference_dataset_collection.gnomad_non_coding_constraint,
        reference_dataset_collection.screen,
    ],
}

VARIANT_ANNOTATIONS = {
    DatasetType.SNV: [
        # shared.rsid,
        shared.rg37_locus,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    DatasetType.MITO: [
        # shared.rsid,
        shared.rg37_locus,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    DatasetType.SV: [
        shared.rg37_locus,
        sv.variant_id,
        shared.xpos,
    ],
    DatasetType.GCNV: [
        gcnv.variant_id,
        gcnv.xpos,
    ],
}


def get_reference_dataset_collection_fields(
    mt: hl.MatrixTable,
    **kwargs: Any,
) -> dict[str, hl.Expression]:
    env = kwargs['env']
    dataset_type = kwargs['dataset_type']
    fields = {
        field_expression.__name__: field_expression(mt, **kwargs)
        for rdc in dataset_type.annotatable_reference_dataset_collections(env)
        for field_expression in REFERENCE_DATASET_COLLECTION_ANNOTATIONS[rdc]
    }
    return {k: v for k, v in fields.items() if v is not None}


def get_variant_fields(
    mt: hl.MatrixTable,
    **kwargs: Any,
) -> dict[str, hl.Expression]:
    dataset_type = kwargs['dataset_type']
    fields = {
        field_expression.__name__: field_expression(mt, **kwargs)
        for field_expression in VARIANT_ANNOTATIONS[dataset_type]
    }
    return {k: v for k, v in fields.items() if v is not None}
