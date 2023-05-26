from typing import Any

import hail as hl

from v03_pipeline.lib.model import Env, ReferenceGenome
from v03_pipeline.lib.model.reference_dataset_collection import (
    ReferenceDatasetCollection,
)
from v03_pipeline.lib.paths import reference_dataset_collection_path


def hgmd(
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    **kwargs: Any,
) -> hl.Table:
    if env == Env.LOCAL:
        return ht

    or all(
        hasattr(ht, rd)
        for rdin ReferenceDatasetCollection.HGMD.reference_datasets
    ):
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
    **kwargs: Any,
) -> hl.Table:
    if all(
        hasattr(ht, rd)
        for rd in ReferenceDatasetCollection.INTERVAL_REFERENCE.reference_datasets
    ):
        return ht
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
                    lambda x: x.screen['region_type_id'],
                )
            ),
        ),
    )