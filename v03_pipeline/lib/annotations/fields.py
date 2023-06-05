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
from v03_pipeline.lib.model import (
    AnnotationType,
    DatasetType,
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path

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
        sample_lookup_table.hom,
    ],
    (DatasetType.SNV, AnnotationType.GENOTYPE_ENTRIES): [
        snv.gq,
        snv.ab,
        snv.dp,
        shared.sample_id,
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


def reference_dataset_collection_tables(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    annotation_type: AnnotationType,
) -> dict[str, hl.Table]:
    if annotation_type != AnnotationType.REFERENCE_DATASET_COLLECTION:
        return {}
    return {
        f'{rdc.value}_ht': hl.read_table(
            valid_reference_dataset_collection_path(
                env,
                reference_genome,
                rdc,
            ),
        )
        for rdc in dataset_type.annotatable_reference_dataset_collections(env)
    }


def get_fields(
    t: hl.Table | hl.MatrixTable,
    annotation_type: AnnotationType,
    **kwargs: Any,
) -> dict[str, hl.Expression]:
    dataset_type = kwargs['dataset_type']
    rdc_hts = reference_dataset_collection_tables(
        kwargs['env'],
        kwargs['reference_genome'],
        dataset_type,
        annotation_type,
    )
    fields = {
        field_expression.__name__: field_expression(t, **kwargs, **rdc_hts)
        for field_expression in ANNOTATION_CONFIG.get(
            (dataset_type, annotation_type),
            [],
        )
    }
    return {k: v for k, v in fields.items() if v is not None}
