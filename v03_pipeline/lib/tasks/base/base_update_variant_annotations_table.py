import hail as hl
import luigi

from v03_pipeline.lib.paths import (
    variant_annotations_table_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
)
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset import (
    UpdatedReferenceDatasetTask,
)


class BaseUpdateVariantAnnotationsTableTask(BaseUpdateTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            self.clone(
                UpdatedReferenceDatasetTask,
                reference_dataset=reference_dataset,
            )
            for reference_dataset in BaseReferenceDataset.for_reference_genome_dataset_type(
                self.reference_genome,
                self.dataset_type,
            )
        ]

    def initialize_table(self) -> hl.Table:
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        return hl.Table.parallelize(
            [],
            key_type,
            key=key_type.fields,
            globals=hl.Struct(
                versions=hl.Struct(),
                enums=hl.Struct(),
                updates=hl.empty_set(
                    hl.tstruct(
                        callset=hl.tstr,
                        project_guid=hl.tstr,
                        remap_pedigree_hash=hl.tint32,
                    ),
                ),
                migrations=hl.empty_array(hl.tstr),
                max_key_=hl.int64(-1),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return ht
