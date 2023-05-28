from __future__ import annotations

from typing import Any

import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import reference_dataset_collection_path
from v03_pipeline.lib.selects import gcnv, reference_dataset_collection, shared, snv, sv

SCHEMA = {
    DatasetType.SNV: [
        snv.original_alt_alleles,
        shared.pos,
        shared.rg37_locus,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    DatasetType.MITO: [
        shared.pos,
        shared.rg37_locus,
        shared.sorted_transcript_consequences,
        shared.variant_id,
        shared.xpos,
    ],
    DatasetType.SV: [
        shared.pos,
        shared.rg37_locus,
        sv.variant_id,
        shared.xpos,
    ],
    DatasetType.GCNV: [
        gcnv.pos,
        gcnv.variant_id,
        gcnv.xpos,
    ],
    ReferenceDatasetCollection.HGMD: [
        reference_dataset_collection.hgmd,
    ],
    ReferenceDatasetCollection.INTERVAL_REFERENCE: [
        reference_dataset_collection.gnomad_non_coding_constraint,
        reference_dataset_collection.screen,
    ],
}


def get_field_expressions(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    liftover_ref_path: str,
    **_ : Any,
) -> dict[str, hl.Expression]:
    # Initialize liftover
    if reference_genome == ReferenceGenome.GRCh38:
        rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
        rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
        if not rg38.has_liftover(rg37):
            rg38.add_liftover(liftover_ref_path, rg37)

    # Initialize reference_hts
    reference_hts = {
        f'{rdc.value}_ht': hl.read_table(
            reference_dataset_collection_path(
                env,
                reference_genome,
                rdc,
            ),
        )
        for rdc in dataset_type.selectable_reference_dataset_collections(env)
    }
    return {
        field_expression.__name__: field_expression(mt)
        for field_expression in SCHEMA[dataset_type]
    }
