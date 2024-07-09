from abc import ABC, abstractmethod

import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class BaseMigration(ABC):
    @staticmethod
    @abstractmethod
    def reference_genome_dataset_types() -> set[tuple[ReferenceGenome, DatasetType]]:
        #
        # The migration pertains to this set of
        # ReferenceGenome, DatasetType pairs.
        #
        pass

    @staticmethod
    @abstractmethod
    def migrate(ht: hl.Table) -> hl.Table:
        pass
