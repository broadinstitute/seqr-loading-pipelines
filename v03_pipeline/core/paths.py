import os

from v03_pipeline.core.definitions import (
    AccessControl,
    DatasetType,
    Env,
    GCSBucket,
    PipelineVersion,
    ReferenceDatasetCollection,
    ReferenceGenome,
    SampleSource,
    SampleType,
    ValidationDatasetCollection,
)

BASE_PROJECTS = 'base/projects'
REMAP_SUFFIX = '_remap.tsv'


def _v02_dag_name(sample_source: SampleSource, sample_type: SampleType) -> str:
    formatted_sample_type = sample_type.value.upper()
    if sample_source == SampleSource.ANVIL:
        return f'AnVIL_{formatted_sample_type}'
    if sample_source == SampleSource.RDG_BROAD_EXTERNAL:
        return f'RDG_{formatted_sample_type}_Broad_External'
    if sample_source == SampleSource.RDG_BROAD_INTERNAL:
        return f'RDG_{formatted_sample_type}_Broad_Internal'
    msg = f'_v02_dag_name unimplemented for {sample_source}'
    raise ValueError(msg)


def _v02_pipeline_prefix(
    reference_genome: ReferenceGenome,
    sample_source: SampleSource,
    sample_type: SampleType,
) -> str:
    return os.path.join(
        GCSBucket.SEQR_DATASETS.value,
        PipelineVersion.V02.value,
        reference_genome.value,
        _v02_dag_name(sample_source, sample_type),
    )


def _v03_reference_data_prefix(
    env: Env,
    root: GCSBucket,
    reference_genome: ReferenceGenome,
):
    if env == Env.DEV:
        root = GCSBucket.SEQR_SCRATCH_TEMP
    return os.path.join(
        root.value,
        reference_genome.value,
        PipelineVersion.V03.value,
    )


def _v03_pipeline_prefix(
    env: Env,
    root: GCSBucket,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    if env == Env.DEV:
        root = GCSBucket.SEQR_SCRATCH_TEMP
    return os.path.join(
        root.value,
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
            GCSBucket.SEQR_LOADING_TEMP,
            reference_genome,
            dataset_type,
        ),
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
            GCSBucket.SEQR_DATASETS,
            reference_genome,
            dataset_type,
        ),
        'projects',
        project_guid,
        'all_samples.ht',
    )


def reference_dataset_collection_path(
    env: Env,
    reference_genome: ReferenceGenome,
    reference_dataset_collection: ReferenceDatasetCollection,
    reference_dataset_collection_version: str,
) -> str:
    root = (
        GCSBucket.SEQR_REFERENCE_DATA
        if reference_dataset_collection.access_control == AccessControl.PUBLIC
        else GCSBucket.SEQR_REFERENCE_DATA_PRIVATE
    )
    return os.path.join(
        _v03_reference_data_prefix(
            env,
            root,
            reference_genome,
        ),
        reference_dataset_collection.value,
        f'{reference_dataset_collection_version}.ht',
    )


def variant_annotations_table_path(
    env: Env,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            env,
            GCSBucket.SEQR_DATASETS,
            reference_genome,
            dataset_type,
        ),
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
            GCSBucket.SEQR_DATASETS,
            reference_genome,
            dataset_type,
        ),
        'lookup.ht',
    )


def validation_dataset_collection_path(
    env: Env,
    reference_genome: ReferenceGenome,
    validation_dataset_collection: ValidationDatasetCollection,
    validation_dataset_collection_version: str,
) -> str:
    return os.path.join(
        _v03_reference_data_prefix(
            env,
            GCSBucket.SEQR_REFERENCE_DATA,
            reference_genome,
        ),
        validation_dataset_collection.value,
        f'{validation_dataset_collection_version}.ht',
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
        f'{_v02_dag_name(sample_source, sample_type)}{REMAP_SUFFIX}',
    )
