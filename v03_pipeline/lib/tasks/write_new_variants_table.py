import functools

import hail as hl
import luigi

from v03_pipeline.lib.annotations.rdc_dependencies import (
    get_rdc_annotation_dependencies,
)
from v03_pipeline.lib.misc.util import callset_project_pairs
from v03_pipeline.lib.model import Env, ReferenceDatasetCollection
from v03_pipeline.lib.paths import (
    new_variants_table_path,
    remapped_and_subsetted_callset_path,
    variant_annotations_table_path,
)
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.update_variant_annotations_table_with_updated_reference_dataset import (
    UpdateVariantAnnotationsTableWithUpdatedReferenceDataset,
)
from v03_pipeline.lib.tasks.update_lookup_table import (
    UpdateLookupTableTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)

VARIANTS_PER_VEP_PARTITION = 1e3


class WriteNewVariantsTableTask(BaseWriteTask):
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

    @property
    def rdc_annotation_dependencies(self) -> dict[str, hl.Table]:
        return get_rdc_annotation_dependencies(self.dataset_type, self.reference_genome)

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_variants_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        if Env.REFERENCE_DATA_AUTO_UPDATE:
            upstream_table_tasks = [
                UpdateVariantAnnotationsTableWithUpdatedReferenceDataset(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                ),
            ]
        else:
            upstream_table_tasks = [
                BaseVariantAnnotationsTableTask(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                ),
            ]
        if self.dataset_type.has_lookup_table:
            # NB: the lookup table task has remapped and subsetted callset tasks as dependencies.
            upstream_table_tasks.extend(
                [
                    UpdateLookupTableTask(
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
                    ),
                ],
            )
        else:
            upstream_table_tasks.extend(
                [
                    WriteRemappedAndSubsettedCallsetTask(
                        self.reference_genome,
                        self.dataset_type,
                        self.sample_type,
                        callset_path,
                        project_guid,
                        project_remap_path,
                        project_pedigree_path,
                        self.ignore_missing_samples_when_subsetting,
                        self.ignore_missing_samples_when_remapping,
                        self.validate,
                    )
                    for (
                        callset_path,
                        project_guid,
                        project_remap_path,
                        project_pedigree_path,
                    ) in callset_project_pairs(
                        self.callset_paths,
                        self.project_guids,
                        self.project_remap_paths,
                        self.project_pedigree_paths,
                    )
                ],
            )
        return upstream_table_tasks

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

    def create_table(self) -> hl.Table:
        callset_hts = [
            hl.read_matrix_table(
                remapped_and_subsetted_callset_path(
                    self.reference_genome,
                    self.dataset_type,
                    callset_path,
                    project_guid,
                ),
            ).rows()
            for (callset_path, project_guid, _, _) in callset_project_pairs(
                self.callset_paths,
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
            )
        ]

        # Drop any fields potentially unshared/unused by the annotations.
        for i, callset_ht in enumerate(callset_hts):
            for row_field in self.dataset_type.optional_row_fields:
                if hasattr(callset_ht, row_field):
                    callset_hts[i] = callset_ht.drop(row_field)

        callset_ht = functools.reduce(
            (lambda ht1, ht2: ht1.union(ht2, unify=True)),
            callset_hts,
        )
        callset_ht = callset_ht.distinct()

        # Identify new variants.
        annotations_ht = hl.read_table(
            variant_annotations_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )
        ht = callset_ht.anti_join(annotations_ht)

        # Join new variants against the reference dataset collections that are not "annotated".
        for rdc in ReferenceDatasetCollection.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            if rdc.requires_annotation:
                continue
            rdc_ht = self.rdc_annotation_dependencies[f'{rdc.value}_ht']
            ht = ht.join(rdc_ht, 'left')

        return ht.annotate_globals(
            updates={
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
        )
