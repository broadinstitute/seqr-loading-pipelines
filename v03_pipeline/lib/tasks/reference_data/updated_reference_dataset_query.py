import hail as hl
import luigi

from v03_pipeline.lib.paths import valid_reference_dataset_query_path
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    ReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset import (
    UpdatedReferenceDatasetTask,
)


@luigi.util.inherits(BaseLoadingPipelineParams)
class UpdatedReferenceDatasetQueryTask(BaseWriteTask):
    reference_dataset_query: ReferenceDatasetQuery = luigi.EnumParameter(
        enum=ReferenceDatasetQuery,
    )

    # Reference Dataset Queries do not include version
    # in the path to allow for simpler reading logic
    # when they are used downstream by the hail search
    # service.
    def complete(self):
        return super().complete() and hl.eval(
            hl.read_table(self.output().path).version
            == self.reference_dataset_query.version(self.reference_genome),
        )

    def requires(self):
        return self.clone(
            UpdatedReferenceDatasetTask,
            reference_dataset=self.reference_dataset_query.requires,
        )

    def output(self):
        return GCSorLocalTarget(
            valid_reference_dataset_query_path(
                self.reference_genome,
                self.dataset_type,
                self.reference_dataset_query,
            ),
        )

    def create_table(self):
        return self.reference_dataset_query.get_ht(
            self.reference_genome,
            self.dataset_type,
            hl.read_table(self.input().path),
        )
