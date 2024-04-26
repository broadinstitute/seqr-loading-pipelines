import hail as hl
import luigi

from v03_pipeline.lib.misc.lookup import (
    remove_project,
)
from v03_pipeline.lib.tasks.base.base_update_lookup_table import (
    BaseUpdateLookupTableTask,
)


class UpdateLookupTableWithDeletedProjectTask(BaseUpdateLookupTableTask):
    project_guid = luigi.Parameter()

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            ~hl.read_table(self.output().path).updates.project_guid.contains(
                self.project_guid,
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        ht = remove_project(ht, self.project_guid)
        return ht.annotate_globals(
            updates=ht.updates.filter(lambda u: u.project_guid != self.project_guid),
        )
