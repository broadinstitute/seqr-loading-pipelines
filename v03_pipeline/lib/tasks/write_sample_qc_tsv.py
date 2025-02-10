import hail as hl
import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.methods.sample_qc import call_sample_qc
from v03_pipeline.lib.misc.io import import_tdr_qc_metrics
from v03_pipeline.lib.paths import sample_qc_tsv_path, tdr_metrics_dir
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.validate_callset import ValidateCallsetTask
from v03_pipeline.lib.tasks.write_tdr_metrics_files import WriteTDRMetricsFilesTask


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
        return [self.clone(ValidateCallsetTask), self.clone(WriteTDRMetricsFilesTask)]

    def run(self):
        callset_mt = hl.read_matrix_table(self.input()[0].path)

        tdr_metrics_ht = None
        for tdr_metrics_file in hfs.ls(
            tdr_metrics_dir(self.reference_genome, self.dataset_type),
        ):
            if not tdr_metrics_ht:
                tdr_metrics_ht = import_tdr_qc_metrics(tdr_metrics_file.path)
                continue
            tdr_metrics_ht = tdr_metrics_ht.union(
                import_tdr_qc_metrics(tdr_metrics_file.path),
            )

        callset_mt = call_sample_qc(
            callset_mt,
            tdr_metrics_ht,
            self.sample_type,
        )
        ht = callset_mt.cols()
        ht.flatten().export(self.output().path)
