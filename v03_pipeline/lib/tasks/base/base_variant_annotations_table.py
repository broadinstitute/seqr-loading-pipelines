from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl

from v03_pipeline.lib.paths import (
    valid_reference_dataset_collection_path,
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

    def requires(self) -> list[luigi.Task]:
        rdcs = (
            self.dataset_type.joinable_reference_dataset_collections(self.env)
            + self.dataset_type.annotatable_reference_dataset_collections
        )
        return [
            HailTableTask(
                valid_reference_dataset_collection_path(
                    self.env,
                    self.reference_genome,
                    rdc,
                ),
            )
            for rdc in rdcs
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

    def update(self, ht: hl.Table) -> hl.Table:
        return ht
