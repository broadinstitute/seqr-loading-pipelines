from typing import Any

import hail as hl

from v03_pipeline.lib.misc import sample_genotypes
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.paths import sample_lookup_table_path


def AC(  # noqa: N802
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    **_: Any,
) -> hl.Expression:
    sample_lookup_ht = hl.read_table(
        sample_lookup_table_path(
            env,
            reference_genome,
            dataset_type,
        ),
    )
    return sample_genotypes.AC(sample_lookup_ht[ht.key])


def AN(  # noqa: N802
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    **_: Any,
) -> hl.Expression:
    sample_lookup_ht = hl.read_table(
        sample_lookup_table_path(
            env,
            reference_genome,
            dataset_type,
        ),
    )
    return sample_genotypes.AN(sample_lookup_ht[ht.key])


def AF(  # noqa: N802
    ht: hl.Table,
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    **_: Any,
) -> hl.Expression:
    sample_lookup_ht = hl.read_table(
        sample_lookup_table_path(
            env,
            reference_genome,
            dataset_type,
        ),
    )
    return sample_genotypes.AF(sample_lookup_ht[ht.key])
