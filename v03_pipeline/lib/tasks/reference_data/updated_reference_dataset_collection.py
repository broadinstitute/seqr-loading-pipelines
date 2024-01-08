import hail as hl
import luigi

from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    update_or_create_joined_ht,
)
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class UpdatedReferenceDatasetCollectionTask(BaseUpdateTask):
    reference_dataset_collection = luigi.EnumParameter(enum=ReferenceDatasetCollection)
    dataset = luigi.OptionalStrParameter(default=None)

    @property
    def _destination_path(self) -> str:
        return valid_reference_dataset_collection_path(
            self.reference_genome,
            self.dataset_type,
            self.reference_dataset_collection,
        )

    def complete(self) -> bool:
        # new class attribute: self.datasets_to_update = []

        # if self.output().exists():
            # datasets_to_update = []
            # read table from destination path
            # for each dataset type or for single dataset type if dataset is not None
                # if reference ht version is newer than config version, update config file with newer version string
                # if reference ht version is older than config version, add dataset type to list of datasets to update
                # repeat for enums, paths, select fields

            # if list of datasets to update is still empty, return True - no updates required

        # else output doesn't exist (base case)
            # self.datasets_to_update = self.datasets_to_update if not empty else reference_dataset_collection.datasets(dataset_type)

        # return False
        pass

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
            self.dataset,
            ht,
        )
