import hail as hl
import luigi

from v03_pipeline.lib.model import Env, ReferenceDatasetCollection
from v03_pipeline.lib.paths import (
    valid_reference_dataset_collection_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, HailTableTask
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection import (
    UpdatedReferenceDatasetCollectionTask,
)


class BaseVariantAnnotationsTableTask(BaseUpdateTask):
    @property
    def rdc_annotation_dependencies(self) -> dict[str, hl.Table]:
        annotation_dependencies = {}
        for rdc in ReferenceDatasetCollection.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            annotation_dependencies[f'{rdc.value}_ht'] = hl.read_table(
                valid_reference_dataset_collection_path(
                    self.reference_genome,
                    self.dataset_type,
                    rdc,
                ),
            )
        return annotation_dependencies

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            (
                UpdatedReferenceDatasetCollectionTask(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                    rdc,
                )
                if Env.REFERENCE_DATA_AUTO_UPDATE
                else HailTableTask(
                    valid_reference_dataset_collection_path(
                        self.reference_genome,
                        self.dataset_type,
                        rdc,
                    ),
                )
            )
            for rdc in ReferenceDatasetCollection.for_reference_genome_dataset_type(
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
                paths=hl.Struct(),
                versions=hl.Struct(),
                enums=hl.Struct(),
                updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return ht

    def annotate_reference_dataset_collection_globals(
        self,
        ht: hl.Table,
    ) -> hl.Table:
        for rdc in ReferenceDatasetCollection.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            rdc_ht = self.rdc_annotation_dependencies[f'{rdc.value}_ht']
            rdc_globals = rdc_ht.index_globals()
            ht = ht.select_globals(
                paths=hl.Struct(
                    **ht.globals.paths,
                    **rdc_globals.paths,
                ),
                versions=hl.Struct(
                    **ht.globals.versions,
                    **rdc_globals.versions,
                ),
                enums=hl.Struct(
                    **ht.globals.enums,
                    **rdc_globals.enums,
                ),
                updates=ht.globals.updates,
            )
        return ht
