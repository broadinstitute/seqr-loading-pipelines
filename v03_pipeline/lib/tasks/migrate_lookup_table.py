import hail as hl
import luigi

import v03_pipeline.migrations.lookup
from v03_pipeline.lib.paths import (
    lookup_table_path,
)
from v03_pipeline.lib.tasks.base.base_migrate import BaseMigrateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class MigrateLookupTableTask(BaseMigrateTask):
    @property
    def migrations_path(self) -> str:
        return v03_pipeline.migrations.lookup.__path__[0]

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            lookup_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def initialize_table(self) -> hl.Table:
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        return hl.Table.parallelize(
            [],
            hl.tstruct(
                **key_type,
                project_stats=hl.tarray(
                    hl.tarray(
                        hl.tstruct(
                            **dict.fromkeys(
                                self.dataset_type.lookup_table_fields_and_genotype_filter_fns,
                                hl.tint32,
                            ),
                        ),
                    ),
                ),
            ),
            key=key_type.fields,
            globals=hl.Struct(
                project_sample_types=hl.empty_array(hl.ttuple(hl.tstr, hl.tstr)),
                project_families=hl.empty_dict(
                    hl.ttuple(hl.tstr, hl.tstr),
                    hl.tarray(hl.tstr),
                ),
                updates=hl.empty_set(
                    hl.tstruct(
                        callset=hl.tstr,
                        project_guid=hl.tstr,
                        remap_pedigree_hash=hl.tint32,
                    ),
                ),
            ),
        )
