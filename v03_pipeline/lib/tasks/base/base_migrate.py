import hail as hl
import luigi

from v03_pipeline.lib.migration.misc import list_migrations
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask


class BaseMigrateTask(BaseUpdateTask):
    migration_name = luigi.Parameter()

    @property
    def migrations_path(self):
        raise NotImplementedError

    def complete(self) -> luigi.Target:
        if super().complete():
            migration = dict(
                list_migrations(self.migrations_path),
            )[self.migration_name]
            if (
                self.reference_genome,
                self.dataset_type,
            ) not in migration.reference_genome_dataset_types:
                return True
            mt = hl.read_table(self.output().path)
            return hl.eval(mt.globals.migrations.index(self.migration_name) >= 0)
        return False

    def update_table(self, ht: hl.Table) -> hl.Table:
        migration = dict(list_migrations(self.migrations_path))[self.migration_name]
        if (
            (self.reference_genome, self.dataset_type)
        ) in migration.reference_genome_dataset_types:
            ht = migration.migrate(ht)
            return ht.annotate_globals(
                migrations=ht.globals.migrations.append(self.migration_name),
            )
        return ht
