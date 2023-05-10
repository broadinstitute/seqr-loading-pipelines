import os
import re
from typing import Literal

from v03_pipeline.core.definitions import (
    AccessControl,
    DatasetType,
    Env,
    ReferenceDatasetCollection,
    ReferenceGenome,
    SampleSource,
    SampleType,
)

BASE_PROJECTS = 'base/projects'
LOCAL_DATA_ROOT = os.environ.get('LOCAL_DATA_ROOT', '/var/seqr')
REMAP_SUFFIX = '_remap.tsv'
SEQR_DATASETS = 'gs://seqr-datasets'
SEQR_LOADING_TEMP = 'gs://seqr-loading-temp'
SEQR_REFERENCE_DATA = 'gs://seqr-reference-data'
SEQR_REFERENCE_DATA_PRIVATE = 'gs://seqr-reference-data-private'
SEQR_SCRATCH_TEMP = 'gs://seqr-scratch-temp'
V02 = 'v02'
V03 = 'v03'


def _v02_dag_name(sample_source: SampleSource, sample_type: SampleType) -> str:
    if sample_source == SampleSource.ANVIL:
        return f'AnVIL_{sample_type.value}'
    if sample_source == SampleSource.RDG_BROAD_EXTERNAL:
        return f'RDG_{sample_type.value}_Broad_External'
    if sample_source == SampleSource.RDG_BROAD_INTERNAL:
        return f'RDG_{sample_type.value}_Broad_Internal'
    msg = f'_v02_dag_name unimplemented for {sample_source}'
    raise ValueError(msg)


def _v02_pipeline_prefix(
    reference_genome: ReferenceGenome,
    sample_source: SampleSource,
    sample_type: SampleType,
) -> str:
    return os.path.join(
        SEQR_DATASETS,
        V02,
        reference_genome.value,
        _v02_dag_name(sample_source, sample_type),
    )


def _v03_pipeline_prefix(
    env: Env,
    remote_root: str,
    reference_genome: ReferenceGenome,
) -> str:
    root = remote_root
    if env == Env.DEV:
        root = SEQR_SCRATCH_TEMP
    elif env == Env.LOCAL:
        root = LOCAL_DATA_ROOT
    return os.path.join(
        root,
        reference_genome.value,
        V03,
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
            SEQR_LOADING_TEMP,
            reference_genome,
        ),
        dataset_type.value,
        'families',
        family_guid,
        'all_samples.ht',
    )


def project_pedigree_path(
    reference_genome: ReferenceGenome,
    sample_source: SampleSource,
    sample_type: SampleType,
    project_guid: str,
) -> str:
    return os.path.join(
        _v02_pipeline_prefix(
            reference_genome,
            sample_source,
            sample_type,
        ),
        BASE_PROJECTS,
        project_guid,
        f'{project_guid}_pedigree.tsv',
    )


def project_remap_path(
    reference_genome: ReferenceGenome,
    sample_source: SampleSource,
    sample_type: SampleType,
    project_guid: str,
) -> str:
    return os.path.join(
        _v02_pipeline_prefix(
            reference_genome,
            sample_source,
            sample_type,
        ),
        BASE_PROJECTS,
        project_guid,
        f'{project_guid}{REMAP_SUFFIX}',
    )


def project_subset_path(
    reference_genome: ReferenceGenome,
    sample_source: SampleSource,
    sample_type: SampleType,
    project_guid: str,
) -> str:
    return os.path.join(
        _v02_pipeline_prefix(
            reference_genome,
            sample_source,
            sample_type,
        ),
        BASE_PROJECTS,
        project_guid,
        f'{project_guid}_ids.txt',
    )


def project_table_path(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    project_guid: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            SEQR_DATASETS,
            reference_genome,
        ),
        dataset_type.value,
        'projects',
        project_guid,
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

def vcf_remap_path(
    reference_genome: ReferenceGenome,
    sample_source: SampleSource,
    vcf_version: str,
) -> str:
    sample_type = SampleType.WGS
    return os.path.join(
        _v02_pipeline_prefix(
            reference_genome,
            sample_source,
            sample_type,
        ),
        vcf_version,
        f'{_v02_dag_name(sample_source, sample_type)}{REMAP_SUFFIX}'
    )
