import functools

import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path


def get_callset_ht(  # noqa: PLR0913
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    callset_paths: list[str],
    project_guids: list[str],
    project_remap_paths: list[str],
    project_pedigree_paths: list[str],
    imputed_sex_paths: list[str] | None,
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
        for (callset_path, project_guid, _, _, _) in callset_project_pairs(
            callset_paths,
            project_guids,
            project_remap_paths,
            project_pedigree_paths,
            imputed_sex_paths,
        )
    ]
    callset_ht = functools.reduce(
        (lambda ht1, ht2: ht1.union(ht2, unify=True)),
        callset_hts,
    )
    return callset_ht.distinct()


def callset_project_pairs(
    callset_paths: list[str],
    project_guids: list[str],
    project_remap_paths: list[str],
    project_pedigree_paths: list[str],
    imputed_sex_paths: list[str] | None,
):
    if len(callset_paths) == len(project_guids):
        return zip(
            callset_paths,
            project_guids,
            project_remap_paths,
            project_pedigree_paths,
            imputed_sex_paths
            if imputed_sex_paths is not None
            else [None] * len(callset_paths),
            strict=True,
        )
    return (
        (
            callset_path,
            project_guid,
            project_remap_path,
            project_pedigree_path,
            imputed_sex_path,
        )
        for callset_path, imputed_sex_path in zip(
            callset_paths,
            imputed_sex_paths
            if imputed_sex_paths is not None
            else [None] * len(callset_paths),
            strict=False,
        )
        for (project_guid, project_remap_path, project_pedigree_path) in zip(
            project_guids,
            project_remap_paths,
            project_pedigree_paths,
            strict=True,
        )
    )
