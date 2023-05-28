from __future__ import annotations

from typing import TYPE_CHECKING, Any

from v03_pipeline.lib.model import DatasetType
from v03_pipeline.lib.selects import gcnv, reference_dataset_collection, shared, snv, sv

if TYPE_CHECKING:
    import hail as hl

SCHEMA = {
    DatasetType.SNV: [
        reference_dataset_collection.hgmd,
        reference_dataset_collection.gnomad_non_coding_constraint,
        reference_dataset_collection.screen,
        shared.rg37_locus,
        snv.original_alt_alleles,
        shared.pos,
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
}


def get_field_expressions(
    mt: hl.MatrixTable,
    **kwargs: Any,
) -> dict[str, hl.Expression]:
    dataset_type = kwargs['dataset_type']
    fields = {
        field_expression.__name__: field_expression(mt, **kwargs)
        for field_expression in SCHEMA[dataset_type]
    }
    return {k: v for k, v in fields.items() if v is not None}
