import luigi
import luigi.util

from v03_pipeline.lib.paths import clickhouse_load_success_file_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.load_complete_run_to_clickhouse import (
    LoadCompleteRunToClickhouse,
)


@luigi.util.inherits(BaseLoadingRunParams)
class WriteClickhouseLoadSuccessFileTask(luigi.Task):
    attempt_id = luigi.IntParameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            clickhouse_load_success_file_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> luigi.Task:
        return self.clone(LoadCompleteRunToClickhouse)

    def run(self):
        with self.output().open('w') as f:
            f.write('')
