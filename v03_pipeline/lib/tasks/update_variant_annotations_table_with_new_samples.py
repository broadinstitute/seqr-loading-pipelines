import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.callsets import callset_project_pairs, get_callset_ht
from v03_pipeline.lib.paths import (
    lookup_table_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.base.base_update_variant_annotations_table import (
    BaseUpdateVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask


class UpdateVariantAnnotationsTableWithNewSamplesTask(BaseUpdateVariantAnnotationsTableTask):
    callset_paths = luigi.ListParameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    ignore_missing_samples_when_subsetting = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    ignore_missing_samples_when_remapping = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    validate = luigi.BoolParameter(
        default=True,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    force = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    liftover_ref_path = luigi.OptionalParameter(
        default='gs://hail-common/references/grch38_to_grch37.over.chain.gz',
        description='Path to GRCh38 to GRCh37 coordinates file',
    )
    run_id = luigi.Parameter()

    def requires(self) -> list[luigi.Task]:
        return [
            *super().requires(),
            WriteNewVariantsTableTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.callset_paths,
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
                self.ignore_missing_samples_when_subsetting,
                self.ignore_missing_samples_when_remapping,
                self.validate,
                self.force,
                self.liftover_ref_path,
                self.run_id,
            ),
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
                                    callset=callset_path,
                                    project_guid=project_guid,
                                ),
                            )
                            for (
                                callset_path,
                                project_guid,
                                _,
                                _,
                            ) in callset_project_pairs(
                                self.callset_paths,
                                self.project_guids,
                                self.project_remap_paths,
                                self.project_pedigree_paths,
                            )
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
                self.callset_paths,
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
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
                    hl.Struct(callset=callset_path, project_guid=project_guid)
                    for (
                        callset_path,
                        project_guid,
                        _,
                        _,
                    ) in callset_project_pairs(
                        self.callset_paths,
                        self.project_guids,
                        self.project_remap_paths,
                        self.project_pedigree_paths,
                    )
                },
            ),
        )
