import os
from typing import Literal

from v03_pipeline.definitions import (
    AccessControl,
    DatasetType,
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
)

SEQR_DATASETS = 'gs://seqr-datasets'
SEQR_LOADING_TEMP = 'gs://seqr-loading-temp'
SEQR_REFERENCE_DATA = 'gs://seqr-reference-data'
SEQR_REFERENCE_DATA_PRIVATE = 'gs://seqr-reference-data-private'
SEQR_SCRATCH_TEMP = 'gs://seqr-scratch-temp'
V03 = 'v03'


def _v03_pipeline_prefix(
    prod_bucket: Literal[SEQR_DATASETS, SEQR_REFERENCE_DATA],
    env: Env,
    reference_genome: ReferenceGenome,
) -> str:
    return os.path.join(
        SEQR_SCRATCH_TEMP if env == Env.DEV else prod_bucket,
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
            SEQR_LOADING_TEMP,
            env,
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
            SEQR_DATASETS,
            env,
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
    prod_bucket = (
        SEQR_REFERENCE_DATA
        if reference_dataset_collection.AccessControl == AccessControl.PUBLIC
        else SEQR_REFERENCE_DATA_PRIVATE
    )
    return os.path.join(
        _v03_pipeline_prefix(
            prod_bucket,
            env,
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
            SEQR_DATASETS,
            env,
            reference_genome,
        ),
        dataset_type.value,
        'annotations.ht',
    )


def variant_lookup_table_path(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> None:
    return os.path.join(
        _v03_pipeline_prefix(
            SEQR_DATASETS,
            env,
            reference_genome,
        ),
        dataset_type.value,
        'lookup.ht',
    )
