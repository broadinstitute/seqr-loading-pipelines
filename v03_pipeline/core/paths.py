import os

from v03_pipeline.core.definitions import (
    AccessControl,
    DatasetType,
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
)

LOCAL_ROOT = '/var/seqr'
SEQR_DATASETS = 'gs://seqr-datasets'
SEQR_LOADING_TEMP = 'gs://seqr-loading-temp'
SEQR_REFERENCE_DATA = 'gs://seqr-reference-data'
SEQR_REFERENCE_DATA_PRIVATE = 'gs://seqr-reference-data-private'
SEQR_SCRATCH_TEMP = 'gs://seqr-scratch-temp'
V03 = 'v03'


def _v03_pipeline_prefix(
    env: Env,
    remote_root: str,
    reference_genome: ReferenceGenome,
) -> str:
    root = remote_root
    if env == Env.DEV:
        root = SEQR_SCRATCH_TEMP
    elif env == Env.LOCAL:
        root = LOCAL_ROOT
    return os.path.join(
        root,
        reference_genome.value,
        V03,
    )


def family_table_path(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    family: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            SEQR_LOADING_TEMP,
            reference_genome,
        ),
        dataset_type.value,
        'families',
        family,
        'all_samples.ht',
    )


def project_table_path(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    project: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            SEQR_DATASETS,
            reference_genome,
        ),
        dataset_type.value,
        'projects',
        project,
        'all_samples.ht',
    )


def reference_dataset_collection_path(
    env: Env,
    reference_genome: ReferenceGenome,
    reference_dataset_collection: ReferenceDatasetCollection,
    version: str,
) -> str:
    remote_root = (
        SEQR_REFERENCE_DATA
        if reference_dataset_collection.access_control == AccessControl.PUBLIC
        else SEQR_REFERENCE_DATA_PRIVATE
    )
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            remote_root,
            reference_genome,
        ),
        reference_dataset_collection.value,
        f'{version}.ht',
    )


def variant_annotations_table_path(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            SEQR_DATASETS,
            reference_genome,
        ),
        dataset_type.value,
        'annotations.ht',
    )


def variant_lookup_table_path(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            SEQR_DATASETS,
            reference_genome,
        ),
        dataset_type.value,
        'lookup.ht',
    )
