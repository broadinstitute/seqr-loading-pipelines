import logging
from typing import ClassVar

import hail as hl
import luigi

from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.reference_data.compare_globals import (
    validate_joined_ht_globals_match_config,
)
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    update_or_create_joined_ht,
)
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget

logger = logging.getLogger(__name__)


class UpdatedReferenceDatasetCollectionTask(BaseUpdateTask):
    reference_dataset_collection = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    dataset = luigi.OptionalStrParameter(default=None)

    _should_validate: bool = True
    _datasets_to_update: ClassVar[list[str]] = []

    @property
    def _destination_path(self) -> str:
        return valid_reference_dataset_collection_path(
            self.reference_genome,
            self.dataset_type,
            self.reference_dataset_collection,
        )

    def complete(self) -> bool:
        self._datasets_to_update.clear()

        if not self.output().exists():
            logger.info(
                f'Creating a new reference dataset collection for {self.reference_genome}, {self.dataset_type}, '
                f'{self.reference_dataset_collection} because it does not exist',
            )
            self._datasets_to_update.extend(
                self.reference_dataset_collection.datasets(self.dataset_type),
            )
            self._should_validate = False
            return False

        if not self._should_validate:
            return super().complete()

        logger.info(
            f'Reading existing reference dataset collection from {self._destination_path}',
        )
        joined_ht = hl.read_table(self._destination_path)
        for dataset in (
            [self.dataset]
            if self.dataset is not None
            else self.reference_dataset_collection.datasets(self.dataset_type)
        ):
            if dataset not in joined_ht.row:
                logger.info(
                    f'Adding missing dataset {dataset} to reference dataset collection',
                )
                self._datasets_to_update.append(dataset)
                continue

            logger.info(
                f'Dataset {dataset} exists in reference dataset collection, validating its globals',
            )
            if not validate_joined_ht_globals_match_config(
                joined_ht,
                dataset,
                self.reference_genome,
            ):
                logger.info(
                    f'Dataset {dataset} did not pass globals validation, re-adding it to reference dataset collection',
                )
                self._datasets_to_update.append(dataset)
                continue

        self._should_validate = False
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
            self._datasets_to_update,
            ht,
        )
