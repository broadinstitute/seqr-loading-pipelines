from typing import Any

import hail as hl

from v03_pipeline.lib.model import (
    AccessControl,
    Env,
    ReferenceGenome,
    ReferenceDatasetCollection,
)
from v03_pipeline.lib.paths import reference_dataset_collection_path

def vep(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    vep_config_json_path: str,
    **kwargs: Any,
) -> hl.Table:
    if hasattr(ht, 'vep'):
        return ht
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

def already_annotated(
    ht: hl.Table,
    env: Env,
    reference_dataset_collection: ReferenceDatasetCollection,
) -> bool:
    return all(
        hasattr(ht, rd) for rd in reference_dataset_collection.reference_datasets
    )

def hgmd(
    ht: hl.Table,
    reference_dataset_collection_ht: hl.Table,
) -> hl.Table:
    return ht.join(reference_dataset_collection_ht, 'outer')

def interval_reference(
    ht: hl.Table,
    reference_dataset_collection_ht: hl.Table,
) -> hl.Table:
    return ht.annotate(
        gnomad_non_coding_constraint=hl.Struct(
            z_score=(
                reference_dataset_collection_ht.index(ht.locus, all_matches=True)
                .filter(
                    lambda x: hl.is_defined(x.gnomad_non_coding_constraint['z_score']),
                )
                .gnomad_non_coding_constraint.z_score.first()
            ),
        ),
        screen=hl.Struct(
            region_type_id=(
                reference_dataset_collection_ht.index(ht.locus, all_matches=True).flatmap(
                    lambda x: x.screen['region_type_id'],
                )
            ),
        ),
    )

def annotate_with_reference_dataset_collections(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    reference_dataset_collections: list[ReferenceDatasetCollection],
) -> hl.Table:
    for reference_dataset_collection in reference_dataset_collections:
        if (
            env == Env.LOCAL
            and reference_dataset_collection.access_control == AccessControl.PRIVATE
        ): 
            continue
        if already_annotated(ht, env, reference_dataset_collection):
            continue
        reference_dataset_collection_ht = hl.read_table(
            reference_dataset_collection_path(
                env, reference_genome, reference_dataset_collection,
            )
        )
    return ht
