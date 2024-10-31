import hail as hl
import luigi

from v03_pipeline.lib.misc.family_entries import remove_family_guids
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget


class UpdateProjectTableWithDeletedFamiliesTask(BaseUpdateTask):
    project_guid = luigi.Parameter()
    family_guids = luigi.ListParameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            project_table_path(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.project_guids[self.project_i],
            ),
        )

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.bind(
                lambda family_guids: (
                    hl.all(
                        hl.array(list(self.family_guids)).map(
                            lambda family_guid: ~family_guids.contains(family_guid),
                        ),
                    )
                ),
                hl.set(
                    hl.read_table(self.output().path).globals.family_guids,
                ),
            ),
        )

    def initialize_table(self) -> hl.Table:
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        return hl.Table.parallelize(
            [],
            hl.tstruct(
                **key_type,
                filters=hl.tset(hl.tstr),
                # NB: entries is missing here because it is untyped
                # until we read the type off of the first callset aggregation.
            ),
            key=key_type.fields,
            globals=hl.Struct(
                family_guids=hl.empty_array(hl.tstr),
                family_samples=hl.empty_dict(hl.tstr, hl.tarray(hl.tstr)),
                updates=hl.empty_set(
                    hl.tstruct(callset=hl.tstr, remap_pedigree_hash=hl.tint32),
                ),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return remove_family_guids(
            ht,
            hl.set(list(self.family_guids)),
        )
