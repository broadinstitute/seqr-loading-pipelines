import luigi
import luigi.util

from v03_pipeline.lib.model.feature_flag import FeatureFlag
from v03_pipeline.lib.paths import pipeline_run_success_file_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.dataproc.rsync_to_seqr_app_dirs import (
    RsyncToSeqrAppDirsTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.run_pipeline import RunPipelineTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteSuccessFileTask(luigi.Task):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            pipeline_run_success_file_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> luigi.Task:
        return (
            self.clone(RsyncToSeqrAppDirsTask)
            if FeatureFlag.RUN_PIPELINE_ON_DATAPROC
            else self.clone(RunPipelineTask)
        )

    def run(self):
        with self.output().open('w') as f:
            f.write('')
