import luigi

import v03_pipeline.migrations.annotations
from v03_pipeline.lib.migration.misc import list_migrations
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.migrate_variant_annotations_table import (
    MigrateVariantAnnotationsTableTask,
)


class MigrateAllVariantAnnotationsTablesTask(luigi.Task):
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
        for _, migration in list_migrations(
            v03_pipeline.migrations.annotations.__path__
        ):
            if (
                self.reference_genome,
                self.dataset_type,
            ) in migration.reference_genome_dataset_types:
                self.dynamic_migration_tasks.append(
                    MigrateVariantAnnotationsTableTask(
                        self.reference_genome,
                        self.dataset_type,
                    ),
                )
        yield self.dynamic_migration_tasks
