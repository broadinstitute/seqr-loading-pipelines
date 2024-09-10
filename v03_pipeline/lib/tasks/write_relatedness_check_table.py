import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.methods.relatedness import call_relatedness
from v03_pipeline.lib.model import CachedReferenceDatasetQuery, Env
from v03_pipeline.lib.paths import (
    relatedness_check_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query import (
    UpdatedCachedReferenceDatasetQuery,
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

    def requires(self) -> luigi.Task:
        requirements = [
            self.clone(ValidateCallsetTask),
        ]
        if Env.ACCESS_PRIVATE_REFERENCE_DATASETS:
            requirements = [
                *requirements,
                (
                    self.clone(
                        UpdatedCachedReferenceDatasetQuery,
                        crdq=CachedReferenceDatasetQuery.GNOMAD_QC,
                    )
                ),
            ]
        return requirements

    def create_table(self) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input()[0].path)
        return call_relatedness(
            callset_mt,
            hl.read_table(self.input()[1].path) if len(self.input()) > 1 else None,
        )
