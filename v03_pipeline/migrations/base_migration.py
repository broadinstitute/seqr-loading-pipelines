from abc import ABC, abstractmethod

import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class BaseMigration(ABC):
    @property
    @abstractmethod
    def reference_genome_dataset_types(
        self,
    ) -> set[tuple[ReferenceGenome, DatasetType]]:
        pass

    @abstractmethod
    def migrate(self, ht: hl.Table) -> hl.Table:
        pass
