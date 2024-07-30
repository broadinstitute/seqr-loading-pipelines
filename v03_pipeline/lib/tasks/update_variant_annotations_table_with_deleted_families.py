import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.update_lookup_table_with_deleted_families import (
    UpdateLookupTableWithDeletedFamiliesTask,
)


class UpdateVariantAnnotationsTableWithDeletedFamiliesTask(
    BaseUpdateVariantAnnotationsTableTask,
):
    project_guid = luigi.Parameter()
    family_guids = luigi.ListParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self) -> luigi.Task | None:
        if self.dataset_type.has_lookup_table:
            return UpdateLookupTableWithDeletedFamiliesTask(
                dataset_type=self.dataset_type,
                reference_genome=self.reference_genome,
                project_guid=self.project_guid,
                family_guids=self.family_guids,
            )
        return None

    def complete(self) -> bool:
        if not self.dataset_type.has_lookup_table:
            return True
        # We don't have the concept of families being present or not present
        # in the annotations table, so we use a done flag to prevent the task
        # from looping over itself.
        return super().complete() and self.done

    def update_table(self, ht: hl.Table) -> hl.Table:
        if not self.dataset_type.has_lookup_table:
            return ht
        lookup_ht = hl.read_table(self.input().path)
        ht = ht.semi_join(lookup_ht)
        ht = ht.annotate(
            **get_fields(
                ht,
                self.dataset_type.lookup_table_annotation_fns,
                lookup_ht=lookup_ht,
                **self.param_kwargs,
            ),
        )
        self.done = True
        return ht
