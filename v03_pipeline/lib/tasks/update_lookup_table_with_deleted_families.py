import hail as hl
import luigi

from v03_pipeline.lib.misc.lookup import (
    remove_family_guids,
)
from v03_pipeline.lib.tasks.base.base_update_lookup_table import (
    BaseUpdateLookupTableTask,
)


class UpdateLookupTableWithDeletedFamiliesTask(BaseUpdateLookupTableTask):
    project_guid = luigi.Parameter()
    family_guids = luigi.ListParameter()

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.bind(
                lambda family_guids: (
                    hl.is_missing(family_guids)  # The project itself is missing
                    | hl.all(
                        hl.array(list(self.family_guids)).map(
                            lambda family_guid: ~family_guids.contains(family_guid),
                        ),
                    )
                ),
                hl.set(
                    hl.read_table(self.output().path).globals.project_families.get(
                        self.project_guid,
                    ),
                ),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return remove_family_guids(
            ht,
            self.project_guid,
            hl.set(list(self.family_guids)),
        )
