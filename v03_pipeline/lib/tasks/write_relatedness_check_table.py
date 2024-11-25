import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.methods.relatedness import call_relatedness
from v03_pipeline.lib.paths import (
    relatedness_check_table_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset import (
    UpdatedReferenceDatasetTask,
)
from v03_pipeline.lib.tasks.validate_callset import ValidateCallsetTask


@luigi.util.inherits(BaseLoadingRunParams)
class WriteRelatednessCheckTableTask(BaseWriteTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            relatedness_check_table_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )

    def requires(self):
        return [
            self.clone(ValidateCallsetTask),
            self.clone(
                UpdatedReferenceDatasetTask,
                reference_dataset=ReferenceDataset.gnomad_qc,
            ),
        ]

    def create_table(self) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input()[0].path)
        return call_relatedness(
            callset_mt,
            hl.read_table(self.input()[1].path) if len(self.input()) > 1 else None,
        )
