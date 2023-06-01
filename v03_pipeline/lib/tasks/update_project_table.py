from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.io import import_callset, import_pedigree, import_remap
from v03_pipeline.lib.misc.pedigree import samples_to_include
from v03_pipeline.lib.misc.sample_ids import remap_sample_ids, subset_samples
from v03_pipeline.lib.model import AnnotationType, SampleFileType, SampleType
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import (
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
    RawFileTask,
    VCFFileTask,
)


class UpdateProjectTableTask(BasePipelineTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples = luigi.BoolParameter(default=False)
    project_guid = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            project_table_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.project_guid,
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
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                **key_type,
                entries=hl.tarray(self.dataset_type.genotype_entries_type),
            ),
            key=key_type.fields,
        )
        return ht.annotate_globals(
            sample_ids=hl.empty_array(hl.tstr),
            updates=hl.empty_set(hl.ttuple(hl.tstr, hl.tstr)),
        )

    def update(self, ht: hl.Table) -> hl.Table:
        callset_mt = import_callset(
            self.callset_path,
            self.env,
            self.reference_genome,
            self.dataset_type,
        )
        project_remap_ht = import_remap(self.project_remap_path)
        pedigree_ht = import_pedigree(self.project_pedigree_path)
        callset_mt = remap_sample_ids(callset_mt, project_remap_ht)
        sample_subset_ht = samples_to_include(pedigree_ht, callset_mt.cols())
        callset_mt = subset_samples(
            callset_mt,
            sample_subset_ht,
            self.ignore_missing_samples,
        )

        # Filter out the samples that we're now loading from the current ht.
        callset_sample_ids = sample_subset_ht.aggregate(
            hl.agg.collect_as_set(sample_subset_ht.s),
        )
        ht = ht.annotate(
            entries=(
                hl.zip_with_index(ht.entries)
                .starmap(
                    lambda i, e: hl.Struct(**e, sample_id=ht.sample_ids[i]),
                )
                .filter(lambda e: ~hl.set(callset_sample_ids).contains(e.sample_id))
            ),
        )

        # Merge the callset entries with the current ht entries
        callset_ht = callset_mt.select_rows(
            entries=hl.agg.collect(
                hl.struct(
                    **get_fields(
                        callset_mt,
                        AnnotationType.GENOTYPE_ENTRIES,
                        **self.param_kwargs,
                    ),
                ),
            ),
        ).rows()
        ht = ht.join(callset_ht, 'outer')
        ht = ht.select(
            entries=hl.sorted(
                (
                    hl.case()
                    .when(hl.is_missing(ht.entries), ht.entries_1)
                    .when(hl.is_missing(ht.entries_1), ht.entries)
                    .default(ht.entries.extend(ht.entries_1))
                ),
                key=lambda e: e.sample_id,
            ),
        )

        # Reannotate
        ht = ht.annotate_globals(
            sample_ids=[
                e.sample_id for e in ht.aggregate(hl.agg.take(ht.entries, 1))[0]
            ],
            updates=ht.updates.add(
                (self.callset_path, self.project_pedigree_path),
            ),
        )
        return ht.select(entries=ht.entries.map(lambda s: s.drop('sample_id')))
