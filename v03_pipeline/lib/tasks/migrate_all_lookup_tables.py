import luigi

import v03_pipeline.migrations.lookup
from v03_pipeline.lib.migration.misc import list_migrations
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.migrate_lookup_table import (
    MigrateLookupTableTask,
)


class MigrateAllLookupTablesTask(luigi.Task):
    reference_genome = luigi.EnumParameter(enum=ReferenceGenome)
    dataset_type = luigi.EnumParameter(enum=DatasetType)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dynamic_migration_tasks = []

    def complete(self) -> bool:
        return len(self.dynamic_migration_tasks) >= 1 and all(
            migration_task.complete() for migration_task in self.migration_task
        )

    def run(self):
        for migration_name, migration in list_migrations(
            v03_pipeline.migrations.lookup.__path__[0],
        ):
            if (
                (self.reference_genome, self.dataset_type)
                in migration.reference_genome_dataset_types
                and self.dataset_type.has_lookup_table
            ):
                self.dynamic_migration_tasks.append(
                    MigrateLookupTableTask(
                        self.reference_genome,
                        self.dataset_type,
                        migration_name=migration_name,
                    ),
                )
        yield self.dynamic_migration_tasks
