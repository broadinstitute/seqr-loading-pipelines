from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.io import import_pedigree, write
from v03_pipeline.lib.misc.pedigree import samples_to_include
from v03_pipeline.lib.misc.sample_entries import globalize_sample_ids
from v03_pipeline.lib.misc.sample_ids import subset_samples
from v03_pipeline.lib.model import AnnotationType
from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallset,
)


class WriteFamilyTableTask(BasePipelineTask):
    callset_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples = luigi.BoolParameter(
        default=False, parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    family_id = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            family_table_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.family_id,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists() and hl.eval(
            hl.read_table(self.output().path).updates.contains(self.callset_path),
        )

    def requires(self) -> luigi.Task:
        return WriteRemappedAndSubsettedCallset(
            self.env,
            self.reference_genome,
            self.dataset_type,
            self.hail_temp_dir,
            self.callset_path,
            self.project_remap_path,
            self.project_pedigree_path,
            self.ignore_missing_samples,
        )

    def run(self) -> None:
        self.init_hail()
        callset_mt = hl.read_matrix_table(self.input().path)
        pedigree_ht = import_pedigree(self.project_pedigree_path)
        sample_subset_ht = samples_to_include(
            pedigree_ht,
            callset_mt.cols(),
            self.family_id,
        )
        if sample_subset_ht.count() == 0:
            msg = f'Family {self.family_id} is invalid.'
            raise RuntimeError(msg)
        callset_mt = subset_samples(
            callset_mt,
            sample_subset_ht,
            self.ignore_missing_samples,
        )
        ht = callset_mt.select_rows(
            entries=hl.sorted(
                hl.agg.collect(
                    hl.struct(
                        **get_fields(
                            callset_mt,
                            AnnotationType.GENOTYPE_ENTRIES,
                            **self.param_kwargs,
                        ),
                    ),
                ),
                key=lambda e: e.sample_id,
            ),
        ).rows()
        ht = globalize_sample_ids(ht)
        ht = ht.annotate_globals(
            updates={self.callset_path},
        )
        write(self.env, ht, self.output().path)
