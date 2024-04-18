import hail as hl
import luigi

from v03_pipeline.lib.misc.lookup import (
    remove_project,
)
from v03_pipeline.lib.tasks.base.base_lookup_table_task import BaseLookupTableTask
from v03_pipeline.lib.tasks.update_variant_annotations_table_with_deleted_project import (
    UpdateVariantAnnotationsTableWithDeletedProjectTask,
)


class UpdateLookupTableWithDeletedProjectTask(BaseLookupTableTask):
    project_guid = luigi.Parameter()

    def requires(self) -> luigi.Task:
        return UpdateVariantAnnotationsTableWithDeletedProjectTask(
            dataset_type=self.dataset_type,
            sample_type=self.sample_type,
            reference_genome=self.reference_genome,
            project_guid=self.project_guid,
        )

    def complete(self) -> bool:
        if super().complete():
            print(hl.eval(hl.read_table(self.output().path).updates.project_guid))
        return super().complete() and hl.eval(
            ~hl.read_table(self.output().path).updates.project_guid.contains(
                self.project_guid,
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        ht = remove_project(ht, self.project_guid)
        return ht.annotate_globals(
            updates=ht.updates.filter(lambda u: u.project_guid != self.project_guid)
        )
