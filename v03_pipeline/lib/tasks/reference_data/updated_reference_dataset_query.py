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
    UpdatedReferenceDataset,
)


@luigi.util.inherits(BaseLoadingRunParams)
class UpdatedReferenceDatasetQuery(BaseWriteTask):
    reference_dataset_query: ReferenceDatasetQuery

    def requires(self):
        return self.clone(
            UpdatedReferenceDataset(
                reference_dataset=self.reference_dataset_query.requires,
            ),
        )

    def output(self):
        return GCSorLocalTarget(
            valid_reference_dataset_path(
                self.reference_genome,
                self.reference_dataset,
            ),
        )

    def create_table(self):
        reference_dataset_ht = hl.read_table(self.input().path)
        ht = self.reference_dataset.get_ht(reference_dataset_ht)
        # enum logic goes here
        return ht.annotate_globals(
            version=self.reference_dataset.version,
            enums=hl.Struct(),  # expect more complex enum logic
        )
