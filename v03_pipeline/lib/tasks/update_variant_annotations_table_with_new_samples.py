import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.callsets import get_callset_ht, get_callset_mt
from v03_pipeline.lib.misc.io import remap_pedigree_hash
from v03_pipeline.lib.paths import (
    lookup_table_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.update_new_variants_with_caids import (
    UpdateNewVariantsWithCAIDsTask,
)
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask


@luigi.util.inherits(BaseLoadingRunParams)
class UpdateVariantAnnotationsTableWithNewSamplesTask(
    BaseUpdateVariantAnnotationsTableTask,
):
    def requires(self) -> list[luigi.Task]:
        return [
            *super().requires(),
            self.clone(UpdateNewVariantsWithCAIDsTask)
            if self.dataset_type.should_send_to_allele_registry
            else self.clone(WriteNewVariantsTableTask),
        ]

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.bind(
                lambda updates: hl.all(
                    [
                        updates.contains(
                            hl.Struct(
                                callset=self.callset_path,
                                project_guid=project_guid,
                                remap_pedigree_hash=remap_pedigree_hash(
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

    def update_table(self, ht: hl.Table) -> hl.Table:
        # Gracefully handle case for on-premises uses
        # where key_ field is not present and migration was not run.
        if not hasattr(ht, 'key_'):
            ht = ht.add_index(name='key_')
            ht = ht.annotate_globals(max_key_=(ht.count() - 1))

        new_variants_ht = hl.read_table(
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

        # Union with the new variants table and annotate with the lookup table.
        ht = ht.union(new_variants_ht, unify=True)
        if self.dataset_type.variant_frequency_annotation_fns:
            callset_ht = get_callset_ht(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
                self.project_guids,
            )
            lookup_ht = None
            if self.dataset_type.has_lookup_table:
                lookup_ht = hl.read_table(
                    lookup_table_path(
                        self.reference_genome,
                        self.dataset_type,
                    ),
                )
                # Variants may have fallen out of the callset and
                # have been removed from the lookup table during modification.
                # Ensure we don't proceed with those variants.
                ht = ht.semi_join(lookup_ht)
            elif self.dataset_type.gt_stats_from_hl_call_stats:
                callset_mt = get_callset_mt(
                    self.reference_genome,
                    self.dataset_type,
                    self.callset_path,
                    self.project_guids,
                )
                callset_mt = callset_mt.annotate_rows(
                    gt_stats=hl.agg.call_stats(callset_mt.GT, callset_mt.alleles),
                )
                callset_ht = callset_mt.rows()

            # new_variants_ht consists of variants present in the new callset, fully annotated,
            # but NOT present in the existing annotations table.
            # callset_variants_ht consists of variants present in the new callset, fully annotated,
            # and either present or not present in the existing annotations table.
            callset_variants_ht = ht.semi_join(callset_ht)
            non_callset_variants_ht = ht.anti_join(callset_ht)
            callset_variants_ht = callset_variants_ht.annotate(
                **get_fields(
                    callset_variants_ht,
                    self.dataset_type.variant_frequency_annotation_fns,
                    lookup_ht=lookup_ht,
                    callset_ht=callset_ht,
                    **self.param_kwargs,
                ),
            )
            ht = non_callset_variants_ht.union(callset_variants_ht, unify=True)
        new_variants_ht_globals = new_variants_ht.index_globals()
        return ht.select_globals(
            versions=new_variants_ht_globals.versions,
            enums=new_variants_ht_globals.enums,
            updates=ht.updates.union(new_variants_ht_globals.updates),
            migrations=ht.migrations,
            max_key_=ht.aggregate(hl.agg.max(ht.key_)),
        )
