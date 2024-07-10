import hail as hl
import luigi

from v03_pipeline.lib.migration.misc import list_migrations
from v03_pipeline.lib.paths import (
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class MigrateVariantAnnotationsTableTask(BaseUpdateTask):
    migration_name = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> luigi.Target:
        if super().complete():
            migration = dict(list_migrations())[self.migration_name]
            if (
                self.reference_genome,
                self.dataset_type,
            ) not in migration.reference_genome_dataset_types:
                return True
            mt = hl.read_table(self.output().path)
            return hl.eval(mt.globals.migrations[-1] == self.migration_name)
        return False

    def initialize_table(self) -> hl.Table:
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        return hl.Table.parallelize(
            [],
            key_type,
            key=key_type.fields,
            globals=hl.Struct(
                paths=hl.Struct(),
                versions=hl.Struct(),
                enums=hl.Struct(),
                updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
                migrations=hl.empty_array(hl.tstr),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        migration = dict(list_migrations())[self.migration_name]
        if (
            (self.reference_genome, self.dataset_type)
        ) in migration.reference_genome_dataset_types:
            ht = migration.migrate(ht)
            return ht.annotate_globals(
                migrations=ht.globals.migrations.append(self.migration_name),
            )
        return ht
