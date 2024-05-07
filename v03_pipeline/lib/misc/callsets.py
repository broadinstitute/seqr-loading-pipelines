import functools

import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path


def get_callset_ht(  # noqa: PLR0913
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    callset_paths: list[str],
    imputed_sex_paths: list[str],
    project_guids: list[str],
    project_remap_paths: list[str],
    project_pedigree_paths: list[str],
):
    callset_hts = [
        hl.read_matrix_table(
            remapped_and_subsetted_callset_path(
                reference_genome,
                dataset_type,
                callset_path,
                project_guid,
            ),
        ).rows()
        for (callset_path, _, project_guid, _, _) in callset_project_pairs(
            callset_paths,
            imputed_sex_paths,
            project_guids,
            project_remap_paths,
            project_pedigree_paths,
        )
    ]

    # Drop any fields potentially unshared/unused by the annotations.
    for i, callset_ht in enumerate(callset_hts):
        for row_field in dataset_type.optional_row_fields:
            if hasattr(callset_ht, row_field):
                callset_hts[i] = callset_ht.drop(row_field)

    callset_ht = functools.reduce(
        (lambda ht1, ht2: ht1.union(ht2, unify=True)),
        callset_hts,
    )
    return callset_ht.distinct()


def callset_project_pairs(
    callset_paths: list[str],
    imputed_sex_paths: list[str],
    project_guids: list[str],
    project_remap_paths: list[str],
    project_pedigree_paths: list[str],
):
    if len(callset_paths) == len(project_guids):
        return zip(
            callset_paths,
            imputed_sex_paths,
            project_guids,
            project_remap_paths,
            project_pedigree_paths,
            strict=True,
        )
    return (
        (
            callset_path,
            imputed_sex_path,
            project_guid,
            project_remap_path,
            project_pedigree_path,
        )
        for callset_path, imputed_sex_path in zip(
            callset_paths, imputed_sex_paths, strict=False
        )
        for (project_guid, project_remap_path, project_pedigree_path) in zip(
            project_guids,
            project_remap_paths,
            project_pedigree_paths,
            strict=True,
        )
    )
