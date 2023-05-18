import os

from v03_pipeline.lib.definitions import (
    DataRoot,
    DatasetType,
    Env,
    PipelineVersion,
    ReferenceGenome,
    Storage,
)


def _v03_pipeline_prefix(
    env: Env,
    storage: Storage,
    root: DataRoot,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    if env == Env.TEST:
        root = DataRoot.TEST_DATASETS
    elif env == Env.LOCAL:
        root = DataRoot.LOCAL_DATASETS
    elif env == Env.DEV or storage == Storage.CHECKPOINT:
        root = DataRoot.SEQR_SCRATCH_TEMP
    return os.path.join(
        root.value,
        'checkpoints' if storage == Storage.CHECKPOINT else '',
        reference_genome.value,
        PipelineVersion.V03.value,
        dataset_type.value,
    )


def family_table_path(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    family_guid: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            Storage.PERMANENT,
            DataRoot.SEQR_LOADING_TEMP,
            reference_genome,
            dataset_type,
        ),
        'families',
        family_guid,
        'samples.ht',
    )


def project_table_path(
    env: Env,
    storage: Storage,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    project_guid: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            storage,
            DataRoot.SEQR_DATASETS,
            reference_genome,
            dataset_type,
        ),
        'projects',
        project_guid,
        'samples.ht',
    )


def variant_annotations_table_path(
    env: Env,
    storage: Storage,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            storage,
            DataRoot.SEQR_DATASETS,
            reference_genome,
            dataset_type,
        ),
        'annotations.ht',
    )


def variant_lookup_table_path(
    env: Env,
    storage: Storage,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            storage,
            DataRoot.SEQR_DATASETS,
            reference_genome,
            dataset_type,
        ),
        'lookup.ht',
    )
