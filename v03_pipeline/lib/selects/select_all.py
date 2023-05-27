import hail as hl

from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.selects import gcnv, reference_dataset_collection, shared, snv, sv

SCHEMA = {
    DatasetType.SNV: [
        reference_dataset_collection.hgmd,
        reference_dataset_collection.gnomad_non_coding_constraint,
        reference_dataset_collection.screen,
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
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    liftover_ref_path: str,
) -> dict[str, hl.Expression]:
    {
        f'{rdc.value}_ht': hl.read_table(
            reference_dataset_collection_path(
                env,
                reference_genome,
                rdc,
            ),
        )
        for rdc in dataset_type.selectable_reference_dataset_collections
    }
    return {
        annotation_fn.__name__: select(mt)
        for annotation_fn in annotation_fns
        if select(mt) is not None
    }


def select_all(mt: hl.MatrixTable, **kwargs) -> hl.MatrixTable:
    return mt.select_rows(**get_select_fields(mt, **kwargs))
