import functools

import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path


def get_callset_ht(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    callset_path: str,
    project_guids: list[str],
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
        for project_guid in project_guids
    ]
    callset_ht = functools.reduce(
        (lambda ht1, ht2: ht1.union(ht2, unify=True)),
        callset_hts,
    )
    return callset_ht.distinct()
