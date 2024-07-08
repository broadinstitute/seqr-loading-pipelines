import hail as hl
import luigi

from v03_pipeline.lib.misc import list_migrations
from v03_pipeline.lib.paths import (
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class BaseMigrateVariantAnnotationsTableTask(BaseUpdateTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> luigi.Target:
        if super().complete():
            mt = hl.read_matrix_table(self.output().path)
            return hasattr(mt, 'migrations') and hl.eval(
                list_migrations() == mt.migrations,
            )
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
                migrations=hl.empty_list(hl.tstr),
            ),
        )
