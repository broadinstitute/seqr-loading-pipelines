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


def _pipeline_prefix(
    root: str,
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        root,
        PipelineVersion.V3_1.value,
        reference_genome.value,
        dataset_type.value,
    )


def _v03_reference_data_prefix(
    access_control: AccessControl,
    reference_genome: ReferenceGenome,
) -> str:
    root = (
        Env.PRIVATE_REFERENCE_DATASETS_DIR_DIR
        if access_control == AccessControl.PRIVATE
        else Env.REFERENCE_DATASETS_DIR
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
    sample_type: SampleType,
    family_guid: str,
) -> str:
    return os.path.join(
        _pipeline_prefix(
            Env.HAIL_SEARCH_DATA_DIR,
            reference_genome,
            dataset_type,
        ),
        'families',
        sample_type.value,
        f'{family_guid}.ht',
    )


def imputed_sex_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    callset_path: str,
) -> str:
    return os.path.join(
        _pipeline_prefix(
            Env.LOADING_DATASETS_DIR,
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
        _pipeline_prefix(
            Env.LOADING_DATASETS_DIR,
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
    sample_type: SampleType,
    project_guid: str,
) -> str:
    return os.path.join(
        _pipeline_prefix(
            Env.HAIL_SEARCH_DATA_DIR,
            reference_genome,
            dataset_type,
        ),
        'projects',
        sample_type.value,
        f'{project_guid}.ht',
    )


def relatedness_check_table_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    callset_path: str,
) -> str:
    return os.path.join(
        _pipeline_prefix(
            Env.LOADING_DATASETS_DIR,
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
        _pipeline_prefix(
            Env.LOADING_DATASETS_DIR,
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
        _pipeline_prefix(
            Env.HAIL_SEARCH_DATA_DIR,
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
        _pipeline_prefix(
            Env.HAIL_SEARCH_DATA_DIR,
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
        _pipeline_prefix(
            Env.LOADING_DATASETS_DIR,
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
        not Env.EXPECT_WES_FILTERS
        or not dataset_type.expect_filters(sample_type)
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
        and reference_dataset_collection.access_control == AccessControl.PRIVATE
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
        _pipeline_prefix(
            Env.HAIL_SEARCH_DATA_DIR,
            reference_genome,
            dataset_type,
        ),
        'annotations.ht',
    )


def variant_annotations_vcf_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
) -> str:
    return os.path.join(
        _pipeline_prefix(
            Env.HAIL_SEARCH_DATA_DIR,
            reference_genome,
            dataset_type,
        ),
        'annotations.vcf.bgz',
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


def clinvar_dataset_path(reference_genome: ReferenceGenome, etag: str) -> str:
    return os.path.join(
        Env.HAIL_TMP_DIR,
        f'clinvar-{reference_genome.value}-{etag}.ht',
    )


def project_remap_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    sample_type: SampleType,
    project_guid: str,
) -> str:
    return os.path.join(
        _pipeline_prefix(
            Env.LOADING_DATASETS_DIR,
            reference_genome,
            dataset_type,
        ),
        'remaps',
        sample_type.value,
        f'{project_guid}.ht',
    )


def project_pedigree_path(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    sample_type: SampleType,
    project_guid: str,
) -> str:
    return os.path.join(
        _pipeline_prefix(
            Env.LOADING_DATASETS_DIR,
            reference_genome,
            dataset_type,
        ),
        'pedigrees',
        sample_type.value,
        f'{project_guid}.ht',
    )


def loading_pipeline_queue_path() -> str:
    return os.path.join(
        Env.LOADING_DATASETS_DIR,
        'loading_pipeline_queue',
        'request.json',
    )
