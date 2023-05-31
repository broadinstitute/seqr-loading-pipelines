from __future__ import annotations

from typing import Any

import hail as hl

from v03_pipeline.lib.model import (
    DatasetType,
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.paths import reference_dataset_collection_path


def hgmd(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    **_: Any,
) -> hl.Expression:
    if (
        ReferenceDatasetCollection.HGMD
        not in dataset_type.annotatable_reference_dataset_collections(env)
    ):
        return None
    hgmd_ht = hl.read_table(
        reference_dataset_collection_path(
            env,
            reference_genome,
            ReferenceDatasetCollection.HGMD,
        ),
    )
    return hgmd_ht[ht.key].hgmd


def gnomad_non_coding_constraint(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    **_: Any,
) -> hl.Expression:
    if (
        ReferenceDatasetCollection.INTERVAL_REFERENCE
        not in dataset_type.annotatable_reference_dataset_collections(env)
    ):
        return None
    interval_reference_ht = hl.read_table(
        reference_dataset_collection_path(
            env,
            reference_genome,
            ReferenceDatasetCollection.INTERVAL_REFERENCE,
        ),
    )
    return hl.Struct(
        z_score=(
            interval_reference_ht.index(ht.locus, all_matches=True)
            .filter(
                lambda x: hl.is_defined(x.gnomad_non_coding_constraint['z_score']),
            )
            .gnomad_non_coding_constraint.z_score.first()
        ),
    )


def screen(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    **_: Any,
) -> hl.Expression:
    if (
        ReferenceDatasetCollection.INTERVAL_REFERENCE
        not in dataset_type.annotatable_reference_dataset_collections(env)
    ):
        return None
    interval_reference_ht = hl.read_table(
        reference_dataset_collection_path(
            env,
            reference_genome,
            ReferenceDatasetCollection.INTERVAL_REFERENCE,
        ),
    )
    return hl.Struct(
        region_type_ids=(
            interval_reference_ht.index(
                ht.locus,
                all_matches=True,
            ).flatmap(
                lambda x: x.screen['region_type_ids'],
            )
        ),
    )
