import hail as hl
import luigi

from luigi_pipeline.lib.hail_tasks import GCSorLocalTarget
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    ReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset import (
    UpdatedReferenceDatasetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class UpdatedReferenceDatasetQueryTask(BaseWriteTask):
    reference_dataset_query: ReferenceDatasetQuery = luigi.EnumParameter(
        enum=ReferenceDatasetQuery,
    )

    def requires(self):
        return self.clone(
            UpdatedReferenceDatasetTask,
            reference_dataset=self.reference_dataset_query.requires,
        )

    def output(self):
        return GCSorLocalTarget(
            valid_reference_dataset_path(
                self.reference_genome,
                self.reference_dataset_query,
            ),
        )

    def create_table(self):
        return self.reference_dataset_query.get_ht(
            self.reference_genome,
            hl.read_table(self.input().path),
        )
