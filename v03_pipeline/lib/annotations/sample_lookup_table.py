from typing import Any

import hail as hl

from v03_pipeline.lib.misc import genotypes
from v03_pipeline.lib.model import DatasetType, Env, ReferenceGenome
from v03_pipeline.lib.paths import sample_lookup_table_path


def AC(  # noqa: N802
    mt: hl.MatrixTable,
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
    return genotypes.AC(sample_lookup_ht[mt.row_key])


def AN(  # noqa: N802
    mt: hl.MatrixTable,
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
    return genotypes.AN(sample_lookup_ht[mt.row_key])


def AF(  # noqa: N802
    mt: hl.MatrixTable,
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
    return genotypes.AF(sample_lookup_ht[mt.row_key])
