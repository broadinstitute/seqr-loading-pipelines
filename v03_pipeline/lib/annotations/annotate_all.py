from __future__ import annotations

from typing import Callable, TYPE_CHECKING

from v03_pipeline.lib.annotations import custom, gcnv, shared, snv, sv
from v03_pipeline.lib.definitions import DatasetType

if TYPE_CHECKING:
    import hail as hl

SCHEMA = {
    DatasetType.SNV: [
        [
            snv.original_alt_alleles,
            shared.pos,
            shared.rg37_locus,
            shared.sorted_transcript_consequences,
            shared.variant_id,
            shared.xpos,
        ],
    ],
    DatasetType.MITO: [
        [
            shared.pos,
            shared.rg37_locus,
            shared.sorted_transcript_consequences,
            shared.variant_id,
            shared.xpos,
        ],
    ],
    DatasetType.SV: [
        [
            shared.pos,
            shared.rg37_locus,
            sv.sorted_transcript_consequences,
            sv.variant_id,
            shared.xpos,
        ],
    ],
    DatasetType.GCNV: [
        [
            gcnv.start,
            gcnv.contig,
        ],
        [
            gcnv.pos,
            gcnv.sorted_transcript_consequences,
            gcnv.variant_id,
            gcnv.xpos,
        ],
    ],
}


def get_annotation_fields(
    annotation_round: list[Callable],
    mt: hl.MatrixTable,
) -> dict[str, hl.Expression]:
    return {
        annotation_fn.__name__: annotation_fn(mt)
        for annotation_fn in annotation_round
        if annotation_fn(mt) is not None
    }


def annotate_all(
    mt: hl.MatrixTable,
    **kwargs,
):
    # Special cases that require hail function calls.
    mt = custom.annotate_old_and_split_multi_hts(mt, **kwargs)
    mt = custom.rg37_locus(mt, **kwargs)
    mt = custom.run_vep(mt, **kwargs)

    dataset_type = kwargs['dataset_type']
    for annotation_round in SCHEMA[dataset_type][:-1]:
        mt = mt.annotate_rows(
            **get_annotation_fields(annotation_round, mt),
        )
    return mt.select_rows(
        **get_annotation_fields(SCHEMA[dataset_type][-1], mt),
    )
