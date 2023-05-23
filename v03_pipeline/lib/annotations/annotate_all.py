from __future__ import annotations

from typing import Callable

import hail as hl

import luigi_pipeline.lib.hail_vep_runners as vep_runners
from v03_pipeline.lib.annotations import gcnv, shared, sv
from v03_pipeline.lib.definitions import DatasetType, Env, ReferenceGenome

SCHEMA = {
    DatasetType.SNV: [
        [
            shared.pos,
            shared.xpos,
            shared.rg37_locus,
            shared.sorted_transcript_consequences,
            shared.variant_id,
        ],
    ],
    DatasetType.MITO: [
        [
            shared.pos,
            shared.xpos,
            shared.rg37_locus,
            shared.sorted_transcript_consequences,
            shared.variant_id,
        ],
    ],
    DatasetType.SV: [
        [
            shared.pos,
            shared.xpos,
            shared.rg37_locus,
            sv.variant_id,
            sv.sorted_transcript_consequences,
        ],
    ],
    DatasetType.GCNV: [
        [
            gcnv.start,
            gcnv.contig,
        ],
        [
            gcnv.pos,
            gcnv.xpos,
            gcnv.variant_id,
            gcnv.sorted_transcript_consequences,
        ],
    ],
}


def annotate_old_and_split_multi_hts(
    mt: hl.MatrixTable,
    dataset_type: DatasetType,
    **kwargs,
) -> hl.MatrixTable:
    if not (dataset_type == DatasetType.SNV or dataset_type == DatasetType.MITO):
        return mt
    return hl.split_multi_hts(
        mt.annotate_rows(locus_old=mt.locus, alleles_old=mt.alleles),
    )


def rg37_locus(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **kwargs,
) -> hl.MatrixTable:
    if reference_genome == ReferenceGenome.GRCh37:
        return mt
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(liftover_ref_path, rg37)
    return mt.annotate_rows(
        rg37_locus=hl.liftover(mt.locus, ReferenceGenome.GRCh37.value),
    )


def run_vep(
    mt: hl.MatrixTable,
    env: Env,
    reference_genome: ReferenceGenome,
    vep_config_json_path: str,
    **kwargs,
) -> hl.MatrixTable:
    vep_runner = (
        vep_runners.HailVEPRunner()
        if env != Env.TEST
        else vep_runners.HailVEPDummyRunner()
    )
    return vep_runner.run(
        mt,
        reference_genome.v02_value,
        vep_config_json_path=vep_config_json_path,
    )


def get_annotation_fields(
    annotation_round: list[Callable],
    mt: hl.MatrixTable,
    **kwargs,
) -> dict[str, hl.Expression]:
    return {
        annotation_fn.__name__: annotation_fn(mt, **kwargs)
        for annotation_fn in annotation_round
        if annotation_fn(mt, **kwargs) is not None
    }


def annotate_all(
    mt: hl.MatrixTable,
    **kwargs,
):
    # Special cases that require hail function calls.
    mt = annotate_old_and_split_multi_hts(mt, **kwargs)
    mt = rg37_locus(mt, **kwargs)
    mt = run_vep(mt, **kwargs)

    dataset_type = kwargs['dataset_type']
    for annotation_round in SCHEMA[dataset_type][:-1]:
        mt = mt.annotate_rows(
            **get_annotation_fields(annotation_round, mt, **kwargs),
        )
    return mt.select_rows(
        **get_annotation_fields(SCHEMA[dataset_type][-1], mt, **kwargs),
    )
