from __future__ import annotations


import luigi

from v03_pipeline.lib.methods.sex_check import call_sex
from v03_pipeline.lib.paths import sex_check_table_path
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_imported_callset import WriteImportedCallsetTask
import hail as hl



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

    def complete(self) -> bool:
        return GCSorLocalTarget(self.output().path).exists()

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
        return call_sex(callset_mt)
