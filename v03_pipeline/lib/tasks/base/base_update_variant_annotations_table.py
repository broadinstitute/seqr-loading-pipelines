import hail as hl
import luigi

from v03_pipeline.lib.annotations.misc import annotate_formatting_annotation_enums
from v03_pipeline.lib.paths import (
    valid_reference_dataset_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import (
    BaseReferenceDataset,
    ReferenceDataset,
    ReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset import (
    UpdatedReferenceDatasetTask,
)
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_query import (
    UpdatedReferenceDatasetQueryTask,
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
        reqs = []
        for reference_dataset in BaseReferenceDataset.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            if isinstance(reference_dataset, ReferenceDatasetQuery):
                reqs.append(
                    self.clone(
                        UpdatedReferenceDatasetQueryTask,
                        reference_dataset_query=reference_dataset,
                    ),
                )
            else:
                reqs.append(
                    self.clone(
                        UpdatedReferenceDatasetTask,
                        reference_dataset=reference_dataset,
                    ),
                )
        return reqs

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
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return ht

    def annotate_globals(
        self,
        ht: hl.Table,
        reference_datasets: set[ReferenceDataset],
    ) -> hl.Table:
        ht = ht.annotate_globals(
            versions=hl.Struct(),
            enums=hl.Struct(),
        )
        for reference_dataset in reference_datasets:
            rd_ht = hl.read_table(
                valid_reference_dataset_path(self.reference_genome, reference_dataset),
            )
            rd_ht_globals = rd_ht.index_globals()
            ht = ht.select_globals(
                versions=hl.Struct(
                    **ht.globals.versions,
                    **{reference_dataset.name: rd_ht_globals.version},
                ),
                enums=hl.Struct(
                    **ht.globals.enums,
                    **{reference_dataset.name: rd_ht_globals.enums},
                ),
                updates=ht.globals.updates,
                migrations=ht.globals.migrations,
            )
        return annotate_formatting_annotation_enums(ht, self.reference_genome, self.dataset_type)
