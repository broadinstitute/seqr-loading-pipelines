from __future__ import annotations

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
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallset,
)
from v03_pipeline.lib.vep import run_vep


class UpdateVariantAnnotationsTableWithNewSamplesTask(BaseVariantAnnotationsTableTask):
    callset_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    project_guid = luigi.Parameter()
    vep_config_json_path = luigi.OptionalParameter(
        default=None,
        description='Path of hail vep config .json file',
    )

    def requires(self) -> list[luigi.Task]:
        return [
            *super().requires(),
            WriteRemappedAndSubsettedCallset(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.hail_temp_dir,
                self.callset_path,
                self.project_remap_path,
                self.project_pedigree_path,
                self.ignore_missing_samples,
            ),
            UpdateSampleLookupTableTask(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.hail_temp_dir,
                self.callset_path,
                self.project_remap_path,
                self.project_pedigree_path,
                self.ignore_missing_samples,
                self.project_guid,
            ),
        ]

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(self.output().path).updates.contains(
                (self.callset_path, self.project_pedigree_path),
            ),
        )

    def update(self, ht: hl.Table) -> hl.Table:
        callset_mt = hl.read_matrix_table(
            remapped_and_subsetted_callset_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
                self.project_pedigree_path,
            ),
        )

        # Get new rows, annotate them with vep, format and annotate them,
        # then stack onto the existing variant annotations table.
        # We then re-annotate the entire table with the allele statistics.
        # NB: the `unify=True` on the `union` here gives us the remainder
        # of the fields defined on the existing table but not over the new rows
        # (most importantly, the reference dataset fields).
        new_variants_ht = callset_mt.anti_join_rows(ht).rows()
        new_variants_ht = run_vep(
            new_variants_ht,
            self.env,
            self.reference_genome,
            self.dataset_type,
            self.vep_config_json_path,
        )
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
        ht = ht.union(new_variants_ht, unify=True)
        ht = ht.annotate(
            **get_fields(
                ht,
                AnnotationType.SAMPLE_LOOKUP_TABLE,
                **self.param_kwargs,
            ),
        )
        return ht.annotate_globals(
            updates=ht.updates.add(
                (self.callset_path, self.project_pedigree_path),
            ),
        )
