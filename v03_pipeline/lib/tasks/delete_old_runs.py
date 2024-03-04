import os

import hail as hl

from v03_pipeline.lib.paths import runs_path
from v03_pipeline.lib.tasks.base.base_hail_table_task import BaseHailTableTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget

MIN_SUCCESSFUL_RUNS = 10


class DeleteOldRunsTask(BaseHailTableTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._complete = False

    def complete(self) -> bool:
        return self._complete

    def run(self) -> None:
        run_dir_paths = sorted(
            [
                rd['path']
                for rd in hl.hadoop_ls(
                    runs_path(
                        self.reference_genome,
                        self.dataset_type,
                    ),
                )
                if rd['is_dir']
            ],
        )
        successful_run_dir_paths = [
            run_dir_path
            for run_dir_path in run_dir_paths
            if hl.hadoop_exists(
                os.path.join(
                    run_dir_path,
                    '_SUCCESS',
                ),
            )
        ]
        if len(successful_run_dir_paths) < MIN_SUCCESSFUL_RUNS:
            self._complete = True
            return

        # Delete run dirs until we encounter the first of the N successful runs to keep.
        oldest_successful_run_index = run_dir_paths.index(
            successful_run_dir_paths[-MIN_SUCCESSFUL_RUNS],
        )
        for run_dir_path in run_dir_paths[:oldest_successful_run_index]:
            GCSorLocalTarget(run_dir_path.replace('file:', '')).remove()
        self._complete = True
