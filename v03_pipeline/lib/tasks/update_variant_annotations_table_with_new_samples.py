import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.util import callset_project_pairs
from v03_pipeline.lib.paths import (
    lookup_table_path,
    new_variants_table_path,
)
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask


class UpdateVariantAnnotationsTableWithNewSamplesTask(BaseVariantAnnotationsTableTask):
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
                self.liftover_ref_path,
                self.run_id,
            ),
        ]

    def complete(self) -> bool:
        return super().complete() and hl.eval(
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
            lookup_annotation_dependencies = {
                'lookup_ht': hl.read_table(
                    lookup_table_path(
                        self.reference_genome,
                        self.dataset_type,
                    ),
                ),
            }
            ht = ht.annotate(
                **get_fields(
                    ht,
                    self.dataset_type.lookup_table_annotation_fns,
                    **lookup_annotation_dependencies,
                    **self.param_kwargs,
                ),
            )

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
