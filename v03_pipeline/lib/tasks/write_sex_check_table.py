import hail as hl
import hailtop.fs as hfs
import luigi

from v03_pipeline.lib.misc.io import import_imputed_sex
from v03_pipeline.lib.paths import sex_check_table_path, tdr_metrics_dir
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_tdr_metrics_files import WriteTDRMetricsFilesTask


class WriteSexCheckTableTask(BaseWriteTask):
    callset_path = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            sex_check_table_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )

    def requires(self) -> luigi.Task:
        return self.clone(WriteTDRMetricsFilesTask)

    def create_table(self) -> hl.Table:
        ht = None
        for tdr_metrics_file in hfs.ls(
            tdr_metrics_dir(self.reference_genome, self.dataset_type),
        ):
            if not ht:
                ht = import_imputed_sex(tdr_metrics_file.path)
                continue
            ht = ht.union(import_imputed_sex(tdr_metrics_file.path))
        return ht
