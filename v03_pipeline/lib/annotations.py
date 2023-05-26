from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl

import luigi_pipeline.lib.hail_vep_runners as vep_runners
from v03_pipeline.lib.definitions import (
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import reference_dataset_collection_path

if TYPE_CHECKING:
    import DatasetType


def hgmd(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    **kwargs,
) -> hl.Table:
    if env == Env.LOCAL:
        return ht
    hgmd_ht = hl.read_table(
        reference_dataset_collection_path(
            env,
            reference_genome,
            ReferenceDatasetCollection.HGMD,
        ),
    )
    return ht.join(hgmd_ht, 'outer')


def interval_reference(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    **kwargs,
) -> hl.Table:
    interval_reference_ht = hl.read_table(
        reference_dataset_collection_path(
            env,
            reference_genome,
            ReferenceDatasetCollection.INTERVAL_REFERENCE,
        ),
    )
    return ht.annotate(
        gnomad_non_coding_constraint=hl.Struct(
            z_score=(
                interval_reference_ht.index(ht.locus, all_matches=True)
                .filter(
                    lambda x: hl.is_defined(x.gnomad_non_coding_constraint['z_score']),
                )
                .gnomad_non_coding_constraint.z_score.first()
            ),
        ),
        screen=hl.Struct(
            region_type_id=(
                interval_reference_ht.index(ht.locus, all_matches=True).flatmap(
                    lambda x: x.screen['region_type'],
                )
            ),
        ),
    )


def rg37_locus(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **kwargs,
) -> hl.MatrixTable:
    if reference_genome == ReferenceGenome.GRCh37:
        return ht
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(liftover_ref_path, rg37)
    return ht.annotate(
        rg37_locus=hl.liftover(ht.locus, ReferenceGenome.GRCh37.value),
    )


def vep(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    vep_config_json_path: str,
    **kwargs,
):
    vep_runner = (
        vep_runners.HailVEPRunner()
        if env != Env.TEST
        else vep_runners.HailVEPDummyRunner()
    )
    return vep_runner.run(
        ht,
        reference_genome.v02_value,
        vep_config_json_path=vep_config_json_path,
    )


def annotate_all(
    ht: hl.Table,
    dataset_type: DatasetType,
    **kwargs,
):
    for annotation in dataset_type.annotations:
        ht = annotation(ht)
    return ht
