import os
from typing import List

import hail as hl
import luigi

from v03_pipeline.core.definitions import ReferenceDatasetCollection
from v03_pipeline.core.paths import reference_dataset_collection_path
from v03_pipeline.tasks.files import HailTable


class UpdateVariantAnnotationsTableWithReferenceData(BaseVariantAnnotationsTable):
    reference_dataset_collection = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    reference_dataset_version = luigi.Parameter(
        description='Version of the reference dataset collection',
    )

    def requires(self) -> List[luigi.Task]:
        return [
            HailTable(
                reference_dataset_collection_path(
                    self.env,
                    self.reference_genome,
                    self.reference_dataset_collection,
                    self.reference_dataset_version,
                ),
            ),
        ]

    def complete(self) -> None:
        return super().complete() and hl.eval(
            hl.read_table(self.path).globals.reference_datasets.contains(
                reference_dataset_collection_path(
                    self.env,
                    self.reference_genome,
                    self.reference_dataset_collection,
                    self.reference_dataset_version,
                ),
            ),
        )
