import hail as hl
import luigi

from luigi_pipeline.lib.hail_tasks import GCSorLocalTarget
from v03_pipeline.lib.paths import valid_reference_dataset_path
from v03_pipeline.lib.reference_data.reference_dataset import ReferenceDataset
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask


@luigi.util.inherits(BaseLoadingRunParams)
class UpdatedReferenceDataset(BaseUpdateTask):
    reference_dataset: ReferenceDataset

    def output(self):
        return GCSorLocalTarget(
            valid_reference_dataset_path(
                self.reference_genome,
                self.reference_dataset,
            ),
        )

    def initialize_table(self):
        key_type = self.reference_dataset.table_key_type(
            self.reference_genome,
        )
        return hl.Table.parallelize(
            [],
            key_type,
            key=key_type.fields,
            globals=hl.Struct(
                version=hl.missing(hl.tstr),
                enums=hl.Struct(),
            ),
        )

    def update_table(self, ht):
        ht_version = hl.eval(ht.globals).version

        if ht_version != self.reference_dataset.version:
            ht = self.reference_dataset.load_parsed_dataset_func(
                self.reference_dataset.raw_dataset_path,
                self.reference_genome,
            )
            # selects logic goes here
            # enum logic goes here
            ht = ht.annotate_globals(
                version=self.reference_dataset.version,
                enums=hl.Struct(),  # expect more complex enum logic
            )

        return ht
