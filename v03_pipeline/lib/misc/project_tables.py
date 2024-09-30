import os

import hail as hl
import hailtop.fs as hfs

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import project_table_path


def get_valid_project_tables(
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    project_guid: str,
) -> set[tuple[hl.Table, SampleType]]:
    project_tables = set()
    for sample_type in SampleType:
        project_ht_path = project_table_path(
            reference_genome,
            dataset_type,
            sample_type,
            project_guid,
        )
        if hfs.exists(project_ht_path) and hfs.exists(
            os.path.join(project_ht_path, '_SUCCESS'),
        ):
            project_ht = hl.read_table(project_ht_path)
            project_tables.add((project_ht, sample_type))
    if len(project_tables) == 0:
        msg = f'No project tables found for {project_guid}'
        raise RuntimeError(msg)
    return project_tables
