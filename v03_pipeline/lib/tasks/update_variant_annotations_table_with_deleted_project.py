import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.lookup import (
    remove_project,
)
from v03_pipeline.lib.paths import (
    lookup_table_path,
)
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import HailTableTask


class UpdateVariantAnnotationsTableWithDeletedProjectTask(
    BaseVariantAnnotationsTableTask,
):
    project_guid = luigi.Parameter()

    def requires(self) -> luigi.Task:
        # Do not call super().requires() here to avoid
        # any reference data requirements.
        return HailTableTask(
            lookup_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
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
                updates=ht.updates.filter(lambda u: u.project_guid == self.project_guid),
            )
        lookup_ht = hl.read_table(
            lookup_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )
        project_i = hl.eval(lookup_ht.globals.project_guids.index(self.project_guid))
        if project_i is None:
            return ht
        lookup_ht = lookup_ht.filter(hl.len(lookup_ht.project_stats[project_i]) > 0)
        lookup_ht = remove_project(lookup_ht, self.project_guid)
        project_variants_ht = ht.semi_join(lookup_ht)
        project_variants_ht = project_variants_ht.annotate(
            **get_fields(
                project_variants_ht,
                self.dataset_type.lookup_table_annotation_fns,
                lookup_ht=hl.read_table(
                    lookup_table_path(
                        self.reference_genome,
                        self.dataset_type,
                    ),
                ),
                **self.param_kwargs,
            ),
        )
        ht = ht.anti_join(lookup_ht)
        ht = ht.union(project_variants_ht, unify=True)
        return ht.annotate_globals(
            updates=ht.updates.filter(lambda u: u.project_guid == self.project_guid),
        )
