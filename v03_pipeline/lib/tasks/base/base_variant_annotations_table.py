from functools import cached_property

import hail as hl
import luigi

from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.paths import (
    sample_lookup_table_path,
    valid_reference_dataset_collection_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.reference_data.gencode.mapping_gene_ids import load_gencode
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection import (
    UpdatedReferenceDatasetCollectionTask,
)

GENCODE_RELEASE = 42


class BaseVariantAnnotationsTableTask(BaseUpdateTask):
    @cached_property
    def annotation_dependencies(self) -> dict[str, hl.Table]:
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

        if self.dataset_type.has_sample_lookup_table:
            annotation_dependencies['sample_lookup_ht'] = hl.read_table(
                sample_lookup_table_path(
                    self.reference_genome,
                    self.dataset_type,
                ),
            )

        if self.dataset_type.has_gencode_mapping:
            annotation_dependencies['gencode_mapping'] = hl.literal(
                load_gencode(GENCODE_RELEASE, ''),
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
            UpdatedReferenceDatasetCollectionTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                rdc,
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
