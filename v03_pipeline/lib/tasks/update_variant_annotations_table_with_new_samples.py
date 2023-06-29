from __future__ import annotations

import functools

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.model import AnnotationType
from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path
from v03_pipeline.lib.tasks.base.base_variant_annotations_table import (
    BaseVariantAnnotationsTableTask,
)
from v03_pipeline.lib.tasks.update_sample_lookup_table import (
    UpdateSampleLookupTableTask,
)
from v03_pipeline.lib.vep import run_vep


class UpdateVariantAnnotationsTableWithNewSamplesTask(BaseVariantAnnotationsTableTask):
    callset_path = luigi.Parameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    ignore_missing_samples = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    vep_config_json_path = luigi.OptionalParameter(
        default=None,
        description='Path of hail vep config .json file',
    )

    def requires(self) -> list[luigi.Task]:
        return [
            *super().requires(),
            UpdateSampleLookupTableTask(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.hail_temp_dir,
                self.callset_path,
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
                self.ignore_missing_samples,
            ),
        ]

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.all(
                [
                    hl.read_table(self.output().path).updates.contains(
                        hl.Struct(callset=self.callset_path, project_guid=project_guid),
                    )
                    for project_guid in self.project_guids
                ],
            ),
        )

    def update(self, ht: hl.Table) -> hl.Table:
        callset_hts = [
            hl.read_matrix_table(
                remapped_and_subsetted_callset_path(
                    self.env,
                    self.reference_genome,
                    self.dataset_type,
                    self.callset_path,
                    project_guid,
                ),
            ).rows()
            for project_guid in self.project_guids
        ]
        callset_ht = functools.reduce(
            (lambda ht1, ht2: ht1.union(ht2, unify=True)),
            callset_hts,
        )

        # 1) Get new rows and annotate with vep
        new_variants_ht = callset_ht.anti_join(ht)
        new_variants_ht = run_vep(
            new_variants_ht,
            self.env,
            self.reference_genome,
            self.dataset_type,
            self.vep_config_json_path,
        )

        # 2) select down to the formatting annotations fields and
        # any reference dataset collection annotations.
        new_variants_ht = new_variants_ht.select(
            **get_fields(
                new_variants_ht,
                AnnotationType.FORMATTING,
                **self.param_kwargs,
            ),
            **get_fields(
                new_variants_ht,
                AnnotationType.REFERENCE_DATASET_COLLECTION,
                **self.param_kwargs,
            ),
        )

        # 3) Join against the reference dataset collection
        for rdc in dataset_type.joinable_reference_dataset_collections(self.env):
            rdc_ht = hl.read_table(
                valid_reference_dataset_collection_path(
                    self.env,
                    self.reference_genome,
                    rdc,
                ),
            )
            new_variants_ht = new_variants_ht.join(rdc_ht, 'left')


        # 4) Union with the existing variant annotations table
        # and annotate the genotype frequencies.
        ht = ht.union(new_variants_ht, unify=True)
        ht = ht.annotate(
            **get_fields(
                ht,
                AnnotationType.SAMPLE_LOOKUP_TABLE,
                **self.param_kwargs,
            ),
        )

        # 5) Mark the table as updated with these callset/projects pairs.
        return ht.annotate_globals(
            updates=ht.updates.union(
                {
                    hl.Struct(callset=self.callset_path, project_guid=project_guid)
                    for project_guid in self.project_guids
                },
            ),
        )
