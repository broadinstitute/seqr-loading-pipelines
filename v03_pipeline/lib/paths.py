import hashlib
import os
import re

from v03_pipeline.lib.model import (
    AccessControl,
    CachedReferenceDatasetQuery,
    DatasetType,
    Env,
    PipelineVersion,
    ReferenceDatasetCollection,
    ReferenceGenome,
    SampleType,
)


def _v03_pipeline_prefix(
    root: str,
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
    access_control: AccessControl,
    reference_genome: ReferenceGenome,
) -> str:
    root = (
        Env.PRIVATE_REFERENCE_DATASETS
        if access_control == AccessControl.PRIVATE
        else Env.REFERENCE_DATASETS
    )
    return os.path.join(
        root,
        PipelineVersion.V03.value,
        reference_genome.value,
    )


def cached_reference_dataset_query_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    cached_reference_dataset_query: CachedReferenceDatasetQuery,
) -> str:
    return os.path.join(
        _v03_reference_data_prefix(
            AccessControl.PUBLIC,
            reference_genome,
        ),
        dataset_type.value,
        'cached_reference_dataset_queries',
        f'{cached_reference_dataset_query.value}.ht',
    )


def family_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    family_guid: str,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.HAIL_SEARCH_DATA,
            reference_genome,
            dataset_type,
        ),
        'families',
        f'{family_guid}.ht',
    )


def imputed_sex_path(
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
        'imputed_sex',
        f'{hashlib.sha256(callset_path.encode("utf8")).hexdigest()}.tsv',
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
        runs_path(
            reference_genome,
            dataset_type,
        ),
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
            Env.HAIL_SEARCH_DATA,
            reference_genome,
            dataset_type,
        ),
        'projects',
        f'{project_guid}.ht',
    )


def relatedness_check_table_path(
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
        'relatedness_check',
        f'{hashlib.sha256(callset_path.encode("utf8")).hexdigest()}.ht',
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


def lookup_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.HAIL_SEARCH_DATA,
            reference_genome,
            dataset_type,
        ),
        'lookup.ht',
    )


def runs_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.HAIL_SEARCH_DATA,
            reference_genome,
            dataset_type,
        ),
        'runs',
    )


def sex_check_table_path(
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
        'sex_check',
        f'{hashlib.sha256(callset_path.encode("utf8")).hexdigest()}.ht',
    )


def valid_filters_path(
    dataset_type: DatasetType,
    sample_type: SampleType,
    callset_path: str,
) -> str | None:
    if (
        not Env.EXPECT_WES_FILTERS or
        not dataset_type.expect_filters(sample_type)
        or 'part_one_outputs' not in callset_path
    ):
        return None
    return re.sub(
        'part_one_outputs/.*$',
        'part_two_outputs/*.filtered.*.vcf.gz',
        callset_path,
    )


def valid_reference_dataset_collection_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    reference_dataset_collection: ReferenceDatasetCollection,
) -> str | None:
    if (
        not Env.ACCESS_PRIVATE_REFERENCE_DATASETS
        or reference_dataset_collection.access_control == AccessControl.PUBLIC
    ):
        return None
    return os.path.join(
        _v03_reference_data_prefix(
            reference_dataset_collection.access_control,
            reference_genome,
        ),
        dataset_type.value,
        'reference_datasets',
        f'{reference_dataset_collection.value}.ht',
    )


def variant_annotations_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _v03_pipeline_prefix(
            Env.HAIL_SEARCH_DATA,
            reference_genome,
            dataset_type,
        ),
        'annotations.ht',
    )


def new_variants_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    run_id: str,
) -> str:
    return os.path.join(
        runs_path(
            reference_genome,
            dataset_type,
        ),
        run_id,
        'new_variants.ht',
    )
