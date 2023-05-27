import hail as hl

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.selects import gcnv, shared, snv, sv

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
}


def get_select_fields(
    mt: hl.MatrixTable,
    dataset_type: DatasetType,
) -> dict[str, hl.Expression]:
    annotation_fns = SCHEMA[dataset_type]
    return {
        annotation_fn.__name__: annotation_fn(mt)
        for annotation_fn in annotation_fns
        if annotation_fn(mt) is not None
    }


def select_all(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    liftover_ref_path: str,
) -> hl.MatrixTable:
    return mt.select_rows(**get_select_fields(mt, dataset_type))
