from typing import Any

import hail as hl

from v03_pipeline.lib.model import DatasetType
from v03_pipeline.lib.paths import reference_dataset_collection_path
from v03_pipeline.lib.selects import gcnv, reference_dataset_collection, shared, snv, sv

SELECTORS = {
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
    **kwargs: Any,
) -> dict[str, hl.Expression]:
    # NB: destructuring inside the function rather than in the
    # arguments so that kwargs can be passed wholesale to the
    # selection functions.
    env, reference_genome = dataset_type = (
        kwargs['env'],
        kwargs['reference_genome'],
        kwargs['dataset_type'],
    )
    rdc_hts = {
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
        selector.__name__: selector(mt, **kwargs, **rdc_hts)
        for selector in SELECTORS[dataset_type]
        if selector(mt, **kwargs, **rdc_hts) is not None
    }


def select_all(mt: hl.MatrixTable, **kwargs: Any) -> hl.MatrixTable:
    return mt.select_rows(**get_select_fields(mt, **kwargs))
