import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.paths import pipeline_run_success_file_path
from v03_pipeline.lib.tasks import WriteProjectFamilyTablesTask
from v03_pipeline.lib.tasks.base.base_project_info_params import (
    BaseLoadingRunWithProjectInfoParams,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


@luigi.util.inherits(BaseLoadingRunWithProjectInfoParams)
class WriteSuccessFileTask(luigi.Task):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            pipeline_run_success_file_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self):
        return [
            self.clone(
                WriteProjectFamilyTablesTask,
                project_guid=self.project_guids[i],
                project_remap_path=self.project_remap_paths[i],
                project_pedigree_path=self.project_pedigree_paths[i],
            )
            for i in range(len(self.project_guids))
        ]

    def run(self):
        with hfs.open(self.output().path, mode='w') as f:
            f.write('')
