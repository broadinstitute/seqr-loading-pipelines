from abc import ABC, abstractmethod

import hail as hl

from v03_pipeline.lib.model import DatasetType, ReferenceGenome


class BaseMigration(ABC):
    reference_genome_dataset_types: frozenset[
        tuple[ReferenceGenome, DatasetType]
    ] = None

    @staticmethod
    @abstractmethod
    def migrate(ht: hl.Table, **kwargs) -> hl.Table:
        pass
