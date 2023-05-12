import os

import luigi

from v03_pipeline.core.paths import variant_annotations_table_path
from v03_pipeline.tasks.base_pipeline_task import BasePipelineTask
from v03_pipeline.tasks.files import GCSorLocalTarget


class BaseVariantAnnotationsTable(BasePipelineTask):
    @property
    def _variant_annotations_table_path(self) -> str:
        return variant_annotations_table_path(
            self.env,
            self.reference_genome,
            self.dataset_type,
        )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self._variant_annotations_table_path)

    def complete(self) -> bool:
        return GCSorLocalTarget(
            os.path.join(self._variant_annotations_table_path, '_SUCCESS'),
        ).exists()
