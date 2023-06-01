from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.io import import_callset, import_pedigree, import_remap
from v03_pipeline.lib.misc.pedigree import samples_to_include
from v03_pipeline.lib.misc.sample_ids import remap_sample_ids, subset_samples
from v03_pipeline.lib.model import AnnotationType, SampleFileType, SampleType
from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import (
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
    RawFileTask,
    VCFFileTask,
)


class WriteFamilyTableTask(BasePipelineTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples = luigi.BoolParameter(default=False)
    family_guid = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            family_table_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.family_guid,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists() and hl.eval(
            hl.read_table(self.output().path).updates.contains(
                (self.callset_path, self.project_pedigree_path),
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            VCFFileTask(self.callset_path)
            if self.dataset_type.sample_file_type == SampleFileType.VCF
            else RawFileTask(self.callset_path),
            RawFileTask(self.project_remap_path),
            RawFileTask(self.project_pedigree_path),
        ]

    def initialize_table(self) -> hl.Table:
        pass

    def update(self, _: hl.Table) -> hl.Table:
        # Family Tables are initialized to empty even if they already exist
        # because the entire family should be contained in the callset.
        callset_mt = import_callset(
            self.callset_path,
            self.env,
            self.reference_genome,
            self.dataset_type,
        )
        project_remap_ht = import_remap(self.project_remap_path)
        pedigree_ht = import_pedigree(self.project_pedigree_path)
        callset_mt = remap_sample_ids(callset_mt, project_remap_ht)
        sample_subset_ht = samples_to_include(
            pedigree_ht, callset_mt.cols(), self.family_guid,
        )
        if sample_subset_ht.count() == 0:
            msg = f'Family {self.family_guid} is invalid.'
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
        ht = ht.annotate_globals(
            sample_ids=[
                e.sample_id for e in ht.aggregate(hl.agg.take(ht.entries, 1))[0]
            ],
            updates=set([(self.callset_path, self.project_pedigree_path)])

        )
        return ht.select(entries=ht.entries.map(lambda s: s.drop('sample_id')))
