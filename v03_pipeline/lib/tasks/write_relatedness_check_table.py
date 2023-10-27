import hail as hl
import luigi

from v03_pipeline.lib.methods.relatedness import call_relatedness
from v03_pipeline.lib.model import CachedReferenceDatasetQuery, Env
from v03_pipeline.lib.paths import (
    relatedness_check_table_path,
    valid_cached_reference_dataset_query_path,
)
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, HailTableTask
from v03_pipeline.lib.tasks.write_imported_callset import WriteImportedCallsetTask


class WriteRelatednessCheckTableTask(BaseWriteTask):
    callset_path = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            relatedness_check_table_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )

    def requires(self) -> luigi.Task:
        requirements = [
            WriteImportedCallsetTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.callset_path,
            ),
        ]
        if Env.ACCESS_PRIVATE_DATASETS:
            requirements = [
                *requirements,
                HailTableTask(
                    valid_cached_reference_dataset_query_path(
                        self.reference_genome,
                        CachedReferenceDatasetQuery.GNOMAD_QC,
                    ),
                ),
            ]
        return requirements

    def create_table(self) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input()[0].path)
        return call_relatedness(
            callset_mt,
            hl.read_table(self.input()[1].path) if len(self.input()) > 1 else None,
        )
