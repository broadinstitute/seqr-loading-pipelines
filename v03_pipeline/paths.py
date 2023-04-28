import os

from v03_pipeline.constants import (
    SEQR_DATASETS,
    SEQR_LOADING_TEMP,
    SEQR_SCRATCH_TEMP,
    V03,
    DatasetType,
    Env,
    ReferenceGenome,
)


def family_table_path(env: Env, reference_genome: ReferenceGenome, dataset_type: DatasetType, family: str) -> str:
    return os.path.join(
        SEQR_SCRATCH_TEMP if env == Env.DEV else SEQR_LOADING_TEMP,
        reference_genome.value,
        V03,
        dataset_type.value,
        'families',
        family,
        'all_samples.ht',
    )

def project_table_path(env: Env, reference_genome: ReferenceGenome, dataset_type: DatasetType, project: str) -> str:
    return os.path.join(
        SEQR_SCRATCH_TEMP if env == Env.DEV else SEQR_DATASETS,
        reference_genome.value,
        V03,
        dataset_type.value,
        'projects',
        project,
        'all_samples.ht',
    )

def variant_annotations_table_path(env: Env, reference_genome: ReferenceGenome, dataset_type: DatasetType) -> str:
    return os.path.join(
        SEQR_SCRATCH_TEMP if env == Env.DEV else SEQR_DATASETS,
        reference_genome.value,
        V03,
        dataset_type.value,
        'annotations.ht',
    )