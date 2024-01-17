from typing import ClassVar

import hail as hl
import luigi

from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    update_or_create_joined_ht,
    validate_joined_ht_globals_match_config,
)
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class UpdatedReferenceDatasetCollectionTask(BaseUpdateTask):
    reference_dataset_collection = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    dataset = luigi.OptionalStrParameter(default=None)

    _datasets_to_update: ClassVar[set[str]] = set()

    @property
    def _destination_path(self) -> str:
        return valid_reference_dataset_collection_path(
            self.reference_genome,
            self.dataset_type,
            self.reference_dataset_collection,
        )

    def complete(self) -> bool:
        if not self.output().exists():
            self._datasets_to_update.update(
                self.reference_dataset_collection.datasets(self.dataset_type),
            )
            return False

        joined_ht = hl.read_table(self._destination_path)
        for dataset in (
            [self.dataset]
            if self.dataset is not None
            else self.reference_dataset_collection.datasets(self.dataset_type)
        ):
            if dataset not in joined_ht.row:
                self._datasets_to_update.add(dataset)
                continue

            if not validate_joined_ht_globals_match_config(
                joined_ht, dataset, self.reference_genome,
            ):
                self._datasets_to_update.add(dataset)
                continue

        return len(self._datasets_to_update) == 0

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(self._destination_path)

    def initialize_table(self) -> hl.Table:
        key_type = self.reference_dataset_collection.table_key_type(
            self.reference_genome,
        )
        return hl.Table.parallelize(
            [],
            key_type,
            key=key_type.fields,
            globals=hl.Struct(
                paths=hl.Struct(),
                versions=hl.Struct(),
                enums=hl.Struct(),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return update_or_create_joined_ht(
            self.reference_dataset_collection,
            self.dataset_type,
            self.reference_genome,
            list(self._datasets_to_update),
            ht,
        )
