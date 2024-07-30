import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.callsets import get_callset_ht
from v03_pipeline.lib.paths import (
    lookup_table_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class UpdateVariantAnnotationsTableWithNewSamplesTask(
    BaseUpdateVariantAnnotationsTableTask,
):
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    run_id = luigi.Parameter()

    def requires(self) -> list[luigi.Task]:
        return [
            *super().requires(),
            self.clone(WriteNewVariantsTableTask),
        ]

    def complete(self) -> bool:
        return (
            not self.force
            and super().complete()
            and hl.eval(
                hl.bind(
                    lambda updates: hl.all(
                        [
                            updates.contains(
                                hl.Struct(
                                    callset=self.callset_path,
                                    project_guid=project_guid,
                                    remap_pedigree_hash=remap_pedigree_hash(
                                        self.project_remap_paths[i],
                                        self.project_pedigree_paths[i],
                                    ),
                                ),
                            )
                            for i, project_guid in enumerate(self.project_guids)
                        ],
                    ),
                    hl.read_table(self.output().path).updates,
                ),
            )
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        new_variants_ht = hl.read_table(
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

        # Union with the new variants table and annotate with the lookup table.
        ht = ht.union(new_variants_ht, unify=True)
        if self.dataset_type.has_lookup_table:
            callset_ht = get_callset_ht(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
                self.project_guids,
            )
            # new_variants_ht consists of variants present in the new callset, fully annotated,
            # but NOT present in the existing annotations table.
            # callset_variants_ht consists of variants present in the new callset, fully annotated,
            # and either present or not present in the existing annotations table.
            callset_variants_ht = ht.semi_join(callset_ht)
            ht = ht.anti_join(callset_ht)
            callset_variants_ht = callset_variants_ht.annotate(
                **get_fields(
                    callset_variants_ht,
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
            ht = ht.union(callset_variants_ht, unify=True)

        # Fix up the globals and mark the table as updated with these callset/project pairs.
        ht = self.annotate_globals(ht)
        return ht.annotate_globals(
            updates=ht.updates.union(
                {
                    hl.Struct(
                        callset=self.callset_path,
                        project_guid=project_guid,
                        remap_pedigree_hash=remap_pedigree_hash(
                            self.project_remap_paths[i], self.project_pedigree_paths[i],
                        ),
                    )
                    for i, project_guid in enumerate(self.project_guids)
                },
            ),
        )
