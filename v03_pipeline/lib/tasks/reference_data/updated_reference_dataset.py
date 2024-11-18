import hail as hl
import luigi

from luigi_pipeline.lib.hail_tasks import GCSorLocalTarget
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask


@luigi.util.inherits(BaseLoadingRunParams)
class UpdatedReferenceDatasetTask(BaseWriteTask):
    reference_dataset: ReferenceDataset = luigi.EnumParameter(
        enum=ReferenceDataset,
    )

    def complete(self):
        return super().complete() and hl.eval(
            hl.read_table(self.output().path).version
            == self.reference_dataset.version(self.reference_genome),
        )

    def output(self):
        return GCSorLocalTarget(
            valid_reference_dataset_path(
                self.reference_genome,
                self.reference_dataset,
            ),
        )

    def create_table(self):
        return self.reference_dataset.get_ht(self.reference_genome)
