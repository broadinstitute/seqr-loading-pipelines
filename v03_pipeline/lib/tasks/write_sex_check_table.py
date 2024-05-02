import hail as hl
import luigi

from v03_pipeline.lib.methods.sex_check import impute_sex
from v03_pipeline.lib.paths import sex_check_table_path
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_imported_callset import WriteImportedCallsetTask


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
        return [
            WriteImportedCallsetTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.callset_path,
            ),
        ]

    def create_table(self) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input()[0].path)
        return impute_sex(callset_mt)
