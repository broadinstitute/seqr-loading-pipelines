import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.lookup import (
    remove_family_guids,
)
from v03_pipeline.lib.paths import (
    lookup_table_path,
)
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.files import HailTableTask


class UpdateVariantAnnotationsTableWithDeletedFamiliesTask(
    BaseVariantAnnotationsTableTask,
):
    project_guid = luigi.Parameter()
    family_guids = luigi.ListParameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.done = False

    def requires(self) -> luigi.Task:
        return HailTableTask(
            lookup_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        if not self.dataset_type.has_lookup_table:
            return True
        return super().complete() and (
            # We don't have the concept of families being present or
            # not present in annotations table, thus we check the "lookup"
            # table OR a done flag to prevent the task from looping over itself.
            self.done
            or hl.eval(
                hl.bind(
                    lambda family_guids: (
                        hl.is_missing(family_guids)  # The project itself is missing
                        | hl.all(
                            hl.array(list(self.family_guids)).map(
                                lambda family_guid: ~hl.set(family_guids).contains(
                                    family_guid,
                                ),
                            ),
                        )
                    ),
                    hl.read_table(self.input().path).globals.project_families.get(
                        self.project_guid,
                    ),
                ),
            )
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        if not self.dataset_type.has_lookup_table:
            return ht
        lookup_ht = hl.read_table(self.input().path)
        project_i = hl.eval(lookup_ht.globals.project_guids.index(self.project_guid))
        if project_i is None:
            return ht

        # Filter lookup table to only the rows where any of the requested families are defined
        # (A family may be set to missing during lookup table construction if all values for a family are 0)
        family_guids_set = hl.set(list(self.family_guids))
        family_indexes = (
            hl.enumerate(lookup_ht.globals.project_families[self.project_guid])
            .filter(lambda item: family_guids_set.contains(item[1]))
            .map(lambda item: item[0])
        )
        lookup_ht = lookup_ht.filter(
            hl.any(
                family_indexes.map(
                    lambda i: hl.is_defined(lookup_ht.project_stats[project_i][i]),
                ),
            ),
        )
        lookup_ht = remove_family_guids(lookup_ht, self.project_guid, family_guids_set)
        family_variants_ht = ht.semi_join(lookup_ht)
        family_variants_ht = family_variants_ht.annotate(
            **get_fields(
                family_variants_ht,
                self.dataset_type.lookup_table_annotation_fns,
                lookup_ht=lookup_ht,
                **self.param_kwargs,
            ),
        )
        ht = ht.anti_join(lookup_ht)
        ht = ht.union(family_variants_ht)
        self.done = True
        return ht
