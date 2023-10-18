from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl

from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.paths import (
    valid_reference_dataset_collection_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, HailTableTask

if TYPE_CHECKING:
    import luigi


class BaseVariantAnnotationsTableTask(BaseUpdateTask):
    n_partitions = 200

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            HailTableTask(
                valid_reference_dataset_collection_path(
                    self.reference_genome,
                    self.dataset_type,
                    rdc,
                ),
            )
            for rdc in ReferenceDatasetCollection.for_dataset_type(self.dataset_type)
        ]

    def initialize_table(self) -> hl.Table:
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        return hl.Table.parallelize(
            [],
            key_type,
            key=key_type.fields,
            globals=hl.Struct(
                paths=hl.Struct(),
                versions=hl.Struct(),
                enums=hl.Struct(),
                updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return ht
