import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.update_lookup_table_with_deleted_project import (
    UpdateLookupTableWithDeletedProjectTask,
)


class UpdateVariantAnnotationsTableWithDeletedProjectTask(
    BaseUpdateVariantAnnotationsTableTask,
):
    project_guid = luigi.Parameter()

    def requires(self) -> luigi.Task:
        return UpdateLookupTableWithDeletedProjectTask(
            dataset_type=self.dataset_type,
            sample_type=self.sample_type,
            reference_genome=self.reference_genome,
            project_guid=self.project_guid,
        )

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            ~hl.read_table(self.output().path).updates.project_guid.contains(
                self.project_guid,
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        if not self.dataset_type.has_lookup_table:
            return ht.annotate_globals(
                updates=ht.updates.filter(
                    lambda u: u.project_guid != self.project_guid,
                ),
            )
        lookup_ht = hl.read_table(self.input().path)
        ht = ht.semi_join(lookup_ht)
        ht.annotate(
            **get_fields(
                ht,
                self.dataset_type.lookup_table_annotation_fns,
                lookup_ht=lookup_ht,
                **self.param_kwargs,
            ),
        )
        return ht.annotate_globals(
            updates=ht.updates.filter(lambda u: u.project_guid != self.project_guid),
        )
