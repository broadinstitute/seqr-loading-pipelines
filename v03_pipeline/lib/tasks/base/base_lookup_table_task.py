import hail as hl
import luigi

from v03_pipeline.lib.paths import lookup_table_path
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class BaseLookupTableTask(BaseUpdateTask):
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
                            **{
                                field: hl.tint32
                                for field in self.dataset_type.lookup_table_fields_and_genotype_filter_fns
                            },
                        ),
                    ),
                ),
            ),
            key=key_type.fields,
            globals=hl.Struct(
                project_guids=hl.empty_array(hl.tstr),
                project_families=hl.empty_dict(hl.tstr, hl.tarray(hl.tstr)),
                updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
            ),
        )
