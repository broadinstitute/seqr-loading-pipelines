import hail as hl
import luigi

from v03_pipeline.lib.misc.lookup import (
    remove_family_guids,
)
from v03_pipeline.lib.tasks.base.base_lookup_table_task import BaseLookupTableTask
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_deleted_families import (
    UpdateVariantAnnotationsTableWithDeletedFamiliesTask,
)


class UpdateLookupTableWithDeletedFamiliesTask(BaseLookupTableTask):
    project_guid = luigi.Parameter()
    family_guids = luigi.ListParameter()

    def requires(self) -> luigi.Task:
        return UpdateVariantAnnotationsTableWithDeletedFamiliesTask(
            dataset_type=self.dataset_type,
            sample_type=self.sample_type,
            reference_genome=self.reference_genome,
            project_guid=self.project_guid,
            family_guids=self.family_guids,
        )

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.bind(
                lambda family_guids: (
                    hl.is_missing(family_guids)
                    | hl.all(
                        hl.array(list(self.family_guids)).map(
                            lambda family_guid: ~family_guids.contains(family_guid),
                        ),
                    ),
                ),
                hl.set(
                    hl.read_table(self.output().path).globals.project_families.get(
                        self.project_guid,
                    ),
                ),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        family_guids = hl.eval(ht.globals.project_families.get(self.project_guid))
        if family_guids and set(self.family_guids) == set(family_guids):
            msg = 'Remove project rather than all families in project'
            raise RuntimeError(msg)
        return remove_family_guids(
            ht, self.project_guid, hl.set(list(self.family_guids)),
        )
