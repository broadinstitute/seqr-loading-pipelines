import pkgutil
import re
from abc import ABC, abstractmethod

import hail as hl

from v03_pipeline import migrations
from v03_pipeline.lib.model import DatasetType, ReferenceGenome

MIGRATION_NAME_PATTERN = r'(\d\d\d\d_.*)'


class Migration(ABC):
    @property
    @abstractmethod
    def reference_genome_dataset_types(
        self,
    ) -> set[tuple[ReferenceGenome, DatasetType]]:
        pass

    @abstractmethod
    def migrate(self, ht: hl.Table) -> hl.Table:
        pass


def list_migrations(
    path: str = migrations.__path__,
) -> list[tuple[str, Migration]]:
    migrations = []
    for loader, name, _ in pkgutil.iter_modules([path]):
        match = re.search(MIGRATION_NAME_PATTERN, name)
        if match:
            module = loader.find_module(name).load_module(name)
            print(module.__dict__.values())
            implemented_migration = next(
                (
                    x for x in module.__dict__.values() if issubclass(x, Migration)
                ), None)
            if implemented_migration:
                migrations.append((match.group(1), implemented_migration))
    return sorted(migrations, key = lambda x: x[0])
