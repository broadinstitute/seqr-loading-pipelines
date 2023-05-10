import hail as hl
import luigi

from v03_pipeline.core.definitions import ReferenceDatasetCollection
from v03_pipeline.core.paths import reference_dataset_collection_path
from v03_pipeline.tasks.files import HailTable
from v03_pipeline.tasks.variant_annotations_table.base_variant_annotations_table import (
    BaseVariantAnnotationsTable,
)


class UpdateVariantAnnotationsTableWithReferenceData(BaseVariantAnnotationsTable):
    reference_dataset_collection = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    reference_dataset_collection_version = luigi.Parameter(
        description='Version of the reference dataset collection',
    )

    @property
    def reference_dataset_collection_path(self):
        return reference_dataset_collection_path(
            self.env,
            self.reference_genome,
            self.reference_dataset_collection,
            self.reference_dataset_collection_version,
        )

    def requires(self) -> luigi.Task:
        return (HailTable(self.reference_dataset_collection_path),)

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(
                self.variant_annotations_table_path,
            ).globals.reference_datasets.contains(
                self.reference_dataset_collection_path,
            ),
        )

    def run(self) -> None:
        print('Running UpdateVariantAnnotationsTableWithReferenceData')
