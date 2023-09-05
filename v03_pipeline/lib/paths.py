from __future__ import annotations

import hashlib
import os

from v03_pipeline.lib.model import (
    AccessControl,
    Env,
    DatasetType,
    Env,
    PipelineVersion,
    ReferenceDatasetCollection,
    ReferenceGenome,
)


def _v03_pipeline_prefix(
    root: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        root,
        PipelineVersion.V03.value,
        reference_genome.value,
        dataset_type.value,
    )


def _v03_reference_data_prefix(
    root: Env,
    reference_genome: ReferenceGenome,
) -> str:
    return os.path.join(
        root,
        PipelineVersion.V03.value,
        reference_genome.value,
    )


def family_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    family_guid: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.DATASETS,
            reference_genome,
            dataset_type,
        ),
        'families',
        f'{family_guid}.ht',
    )


def imported_callset_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    callset_path: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.LOADING_DATASETS,
            reference_genome,
            dataset_type,
        ),
        'imported_callsets',
        f'{hashlib.sha256(callset_path.encode("utf8")).hexdigest()}.mt',
    )


def metadata_for_run_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.DATASETS,
            reference_genome,
            dataset_type,
        ),
        'runs',
        run_id,
        'metadata.json',
    )


def project_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    project_guid: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.DATASETS,
            reference_genome,
            dataset_type,
        ),
        'projects',
        f'{project_guid}.ht',
    )


def remapped_and_subsetted_callset_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    callset_path: str,
    project_guid: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.LOADING_DATASETS,
            reference_genome,
            dataset_type,
        ),
        'remapped_and_subsetted_callsets',
        project_guid,
        f'{hashlib.sha256(callset_path.encode("utf8")).hexdigest()}.mt',
    )


def sample_lookup_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.DATASETS,
            reference_genome,
            dataset_type,
        ),
        'lookup.ht',
    )


def valid_reference_dataset_collection_path(
    reference_genome: ReferenceGenome,
    reference_dataset_collection: ReferenceDatasetCollection,
) -> str | None:
    if (
        not Env.ACCESS_PRIVATE_DATASETS
        and reference_dataset_collection.access_control == AccessControl.PRIVATE
    ):
        return None
    root = (
        Env.PRIVATE_REFERENCE_DATASETS
        if reference_dataset_collection.access_control == AccessControl.PRIVATE
        else Env.REFERENCE_DATASETS
    )
    return os.path.join(
        _v03_reference_data_prefix(
            root,
            reference_genome,
        ),
        'reference_datasets',
        f'{reference_dataset_collection.value}.ht',
    )


def variant_annotations_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.DATASETS,
            reference_genome,
            dataset_type,
        ),
        'annotations.ht',
    )
