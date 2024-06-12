import hail as hl
import luigi

from v03_pipeline.lib.misc.io import import_imputed_sex
from v03_pipeline.lib.paths import sex_check_table_path
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, RawFileTask


class WriteSexCheckTableTask(BaseWriteTask):
    callset_path = luigi.Parameter()
    imputed_sex_path = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            sex_check_table_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )

    def requires(self) -> luigi.Task:
        return RawFileTask(self.imputed_sex_path)

    def create_table(self) -> hl.Table:
        return import_imputed_sex(self.input().path)
