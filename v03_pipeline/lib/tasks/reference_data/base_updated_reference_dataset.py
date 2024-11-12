import hail as hl
import luigi

from luigi_pipeline.lib.hail_tasks import GCSorLocalTarget
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask


@luigi.util.inherits(BaseLoadingRunParams)
class UpdatedReferenceDataset(BaseWriteTask):
    reference_dataset: ReferenceDataset

    def output(self):
        return GCSorLocalTarget(
            valid_reference_dataset_path(
                self.reference_genome,
                self.reference_dataset,
            ),
        )

    def create_table(self):
        ht = self.reference_dataset.get_ht(
            self.reference_dataset.raw_dataset_path,
            self.reference_genome,
        )
        # enum logic goes here
        return ht.annotate_globals(
            version=self.reference_dataset.version,
            enums=hl.Struct(),  # expect more complex enum logic
        )
