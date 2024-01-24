import logging
from typing import ClassVar

import hail as hl
import luigi

from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.reference_data.compare_globals import (
    get_datasets_to_update,
)
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    update_or_create_joined_ht,
)
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget

logger = logging.getLogger(__name__)


class UpdatedReferenceDatasetCollectionTask(BaseUpdateTask):
    reference_dataset_collection = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    _datasets_to_update: ClassVar[list[str]] = []

    def complete(self) -> bool:
        self._datasets_to_update.clear()
        all_datasets_for_collection = self.reference_dataset_collection.datasets(
            self.dataset_type,
        )

        if not super().complete():
            logger.info('Creating a new reference dataset collection')
            self._datasets_to_update.extend(all_datasets_for_collection)
            return False

        joined_ht = hl.read_table(self.output().path)
        self._datasets_to_update.extend(
            get_datasets_to_update(
                joined_ht,
                all_datasets_for_collection,
                self.reference_genome,
            ),
        )

        logger.info(f'Datasets to update: {self._datasets_to_update}')
        return not self._datasets_to_update

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            valid_reference_dataset_collection_path(
                self.reference_genome,
                self.dataset_type,
                self.reference_dataset_collection,
            ),
        )

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
            self._datasets_to_update,
            ht,
        )
