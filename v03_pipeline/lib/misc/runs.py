from collections import defaultdict

import hailtop.fs as hfs

from v03_pipeline.lib.model.dataset_type import DatasetType
from v03_pipeline.lib.paths import (
    clickhouse_load_fail_file_path,
    clickhouse_load_success_file_path,
    pipeline_run_success_file_path,
)


def get_run_ids() -> tuple[defaultdict, defaultdict, defaultdict]:
    successful_pipeline_runs, successful_clickhouse_loads, failed_clickhouse_loads = (
        defaultdict(set),
        defaultdict(set),
        defaultdict(set),
    )
    for dataset_type in DatasetType:
        for reference_genome in dataset_type.reference_genomes:
            successful_pipeline_runs[(reference_genome, dataset_type)] = {
                p.path.split('/')[-2]
                for p in hfs.ls(
                    pipeline_run_success_file_path(
                        reference_genome,
                        dataset_type,
                        '*',
                    ),
                )
            }
            successful_clickhouse_loads[(reference_genome, dataset_type)] = {
                p.path.split('/')[-2]
                for p in hfs.ls(
                    clickhouse_load_success_file_path(
                        reference_genome,
                        dataset_type,
                        '*',
                    ),
                )
            }
            failed_clickhouse_loads[(reference_genome, dataset_type)] = {
                p.path.split('/')[-2]
                for p in hfs.ls(
                    clickhouse_load_fail_file_path(
                        reference_genome,
                        dataset_type,
                        '*',
                    ),
                )
            }
    return (
        successful_pipeline_runs,
        successful_clickhouse_loads,
        failed_clickhouse_loads,
    )
