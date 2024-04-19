import hail as hl
import luigi

from v03_pipeline.lib.misc.family_entries import remove_family_guids
from v03_pipeline.lib.tasks.base.base_project_table_task import BaseUpdateProjectTableTask


class UpdateProjectTableWithDeletedFamiliesTask(BaseUpdateProjectTableTask):
    family_guids = luigi.ListParameter()

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

    def update_table(self, ht: hl.Table) -> hl.Table:
        return remove_family_guids(
            ht,
            hl.set(list(self.family_guids)),
        )
