import luigi

from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc import (
    BaseRunJobOnDataprocTask,
)
from v03_pipeline.lib.tasks.run_pipeline import RunPipelineTask


@luigi.util.inherits(BaseLoadingRunParams)
class RunPipelineOnDataprocTask(BaseRunJobOnDataprocTask):
    attempt_id = luigi.IntParameter()

    @property
    def task(self) -> luigi.Task:
        return RunPipelineTask
