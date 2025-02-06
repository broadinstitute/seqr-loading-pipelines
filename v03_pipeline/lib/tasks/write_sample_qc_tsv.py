import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.methods.sample_qc import call_sample_qc
from v03_pipeline.lib.paths import sample_qc_tsv_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.validate_callset import ValidateCallsetTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteSampleQCTsvTask(luigi.Task):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            sample_qc_tsv_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )

    def requires(self):
        return [self.clone(ValidateCallsetTask)]

    def run(self):
        callset_mt = hl.read_matrix_table(self.input()[0].path)
        callset_mt = call_sample_qc(callset_mt)
        ht = callset_mt.cols()
        ht.flatten().export(self.output().path)
