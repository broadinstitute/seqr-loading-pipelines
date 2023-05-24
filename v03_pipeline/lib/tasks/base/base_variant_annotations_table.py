import hail as hl
import luigi

from v03_pipeline.lib.paths import (
    reference_dataset_collection_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget


class BaseVariantAnnotationsTableTask(BasePipelineTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def initialize_table(self) -> hl.Table:
        if self.dataset_type.base_reference_dataset_collection is None:
            return super().initialize_table()
        return hl.read_table(
            reference_dataset_collection_path(
                self.env,
                self.reference_genome,
                self.dataset_type.base_reference_dataset_collection,
            ),
        )

    def update(self, mt: hl.Table) -> hl.Table:
        return mt
