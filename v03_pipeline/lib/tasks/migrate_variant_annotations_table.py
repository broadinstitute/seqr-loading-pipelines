import hail as hl
import luigi

import v03_pipeline.migrations.annotations
from v03_pipeline.lib.paths import (
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_migrate import BaseMigrateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class MigrateVariantAnnotationsTableTask(BaseMigrateTask):
    @property
    def migrations_path(self):
        return v03_pipeline.migrations.annotations.__path__[0]

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

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
                updates=hl.empty_set(
                    hl.tstruct(
                        callset=hl.tstr,
                        project_guid=hl.tstr,
                        remap_pedigree_hash=hl.tstr,
                    ),
                ),
                migrations=hl.empty_array(hl.tstr),
            ),
        )
