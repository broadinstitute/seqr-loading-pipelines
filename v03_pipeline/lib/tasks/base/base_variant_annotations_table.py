from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl

from v03_pipeline.lib.paths import (
    reference_dataset_collection_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import (
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
    HailTableTask,
)

if TYPE_CHECKING:
    import luigi


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

    def requires(self) -> luigi.Task | None:
        if self.dataset_type.base_reference_dataset_collection is None:
            return None
        return HailTableTask(
            reference_dataset_collection_path(
                self.env,
                self.reference_genome,
                self.dataset_type.base_reference_dataset_collection,
            ),
        )

    def initialize_table(self) -> hl.Table:
        if self.dataset_type.base_reference_dataset_collection is None:
            key_type = self.dataset_type.table_key_type(self.reference_genome)
            return hl.Table.parallelize(
                [],
                key_type,
                key=key_type.fields,
            )
        return hl.read_table(
            reference_dataset_collection_path(
                self.env,
                self.reference_genome,
                self.dataset_type.base_reference_dataset_collection,
            ),
        )

    def update(self, mt: hl.Table) -> hl.Table:
        return mt
