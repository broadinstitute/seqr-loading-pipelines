from __future__ import annotations

from typing import Callable, TYPE_CHECKING

from v03_pipeline.lib.annotations import custom, gcnv, shared, snv, sv
from v03_pipeline.lib.definitions import DatasetType

if TYPE_CHECKING:
    import hail as hl

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
        sv.sorted_transcript_consequences,
        sv.variant_id,
        shared.xpos,
    ],
    DatasetType.GCNV: [
        gcnv.pos,
        gcnv.sorted_transcript_consequences,
        gcnv.variant_id,
        gcnv.xpos,
    ],
}


def get_select_fields(
    annotation_fns: list[Callable],
    mt: hl.MatrixTable,
) -> dict[str, hl.Expression]:
    return {
        annotation_fn.__name__: annotation_fn(mt)
        for annotation_fn in annotation_fns
        if annotation_fn(mt) is not None
    }


def annotate_all(
    mt: hl.MatrixTable,
    env: Env,
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
    vep_config_json_path: str,
    liftover_ref_path: str | None,
    **kwargs,
):
    # Special cases that require hail function calls.
    if (dataset_type == DatasetType.SNV or dataset_type == DatasetType.MITO):
        mt = custom.annotate_old_and_split_multi_hts(mt, )
    if dataset_type != DatasetType.GCNV:
        mt = custom.rg37_locus(mt, reference_genome, liftover_ref_path)
    if (dataset_type == DatasetType.SNV or dataset_type == DatasetType.MITO):
        mt = custom.run_vep(mt, env, reference_genome, vep_config_json_path)
    return mt.select_rows(
        **get_select_fields(SCHEMA[dataset_type][-1], mt),
    )
