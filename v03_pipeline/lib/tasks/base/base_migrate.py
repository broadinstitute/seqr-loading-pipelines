import hail as hl
import luigi

from v03_pipeline.lib.migration.misc import list_migrations
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask


class BaseMigrateTask(BaseUpdateTask):
    migration_name = luigi.Parameter()

    @property
    def migrations_path(self) -> str:
        raise NotImplementedError

    def requires(self) -> luigi.Task | None:
        # Require the previous migration
        defined_migrations = [x[0] for x in list_migrations(self.migrations_path)]
        for i, migration in enumerate(defined_migrations):
            if i > 0 and migration == self.migration_name:
                return self.clone(
                    self.__class__,
                    migration_name=defined_migrations[i - 1],
                )
        return None

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
            if not hasattr(mt, 'migrations'):
                return False
            return hl.eval(mt.globals.migrations.index(self.migration_name) >= 0)
        return False

    def update_table(self, ht: hl.Table) -> hl.Table:
        migration = dict(list_migrations(self.migrations_path))[self.migration_name]
        if (
            (self.reference_genome, self.dataset_type)
        ) in migration.reference_genome_dataset_types:
            ht = migration.migrate(ht, **self.param_kwargs)
            return ht.annotate_globals(
                migrations=ht.globals.migrations.append(self.migration_name),
            )
        return ht
