import hail as hl
import luigi

from luigi_pipeline.lib.hail_tasks import GCSorLocalTarget
from v03_pipeline.lib.misc.io import does_file_exist
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_data.reference_dataset import ReferenceDataset
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
        if does_file_exist(self.output().path):
            ht = hl.read_table(self.output().path)
            # todo fix version check
            ht_version = hl.eval(ht.globals).version

            if ht_version == self.reference_dataset.version:
                return ht

        ht = self.reference_dataset.load_parsed_dataset_func(
            self.reference_dataset.raw_dataset_path,
            self.reference_genome,
        )
        # selects logic goes here
        # enum logic goes here
        return ht.annotate_globals(
            version=self.reference_dataset.version,
            enums=hl.Struct(),  # expect more complex enum logic
        )
