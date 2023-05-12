from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.core.definitions import ReferenceDataset, ReferenceDatasetCollection
from v03_pipeline.core.paths import reference_dataset_collection_path
from v03_pipeline.tasks.files import HailTable
from v03_pipeline.tasks.variant_annotations_table.base_variant_annotations_table import (
    BaseVariantAnnotationsTable,
)


class UpdateVariantAnnotationsTableWithReferenceDataset(BaseVariantAnnotationsTable):
    reference_dataset = luigi.EnumParameter(enum=ReferenceDataset)
    reference_dataset_collection = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    reference_dataset_collection_version = luigi.Parameter(
        description='Version of the reference dataset collection',
    )

    @property
    def _reference_dataset_collection_path(self) -> str:
        return reference_dataset_collection_path(
            self.env,
            self.reference_genome,
            self.reference_dataset_collection,
            self.reference_dataset_collection_version,
        )

    @property
    def _completion_token(self) -> tuple[str, str, str]:
        return (
            self.reference_dataset.value,
            self.reference_dataset_collection.value,
            self.reference_dataset_collection_version,
        )

    def requires(self) -> luigi.Task:
        return HailTable(self._reference_dataset_collection_path)

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(
                self._variant_annotations_table_path,
            ).globals.reference_datasets.contains(self._completion_token),
        )

    def validate(self) -> None:
        super().validate()
        if (
            self.reference_dataset
            not in self.reference_dataset_collection.reference_datasets
        ):
            msg = f'{self.reference_dataset.value} is not included in {self.reference_dataset_collection.value}'
            raise self.InvalidParameterError(msg)

    def run(self) -> None:
        super().run()
        print('Running UpdateVariantAnnotationsTableWithReferenceData')
