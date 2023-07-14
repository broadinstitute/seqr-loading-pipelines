from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.sample_entries import (
    deglobalize_sample_ids,
    globalize_sample_ids,
    union_entries_hts,
)
from v03_pipeline.lib.model import AnnotationType
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class UpdateProjectTableTask(BasePipelineTask):
    callset_path = luigi.Parameter()
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )

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
                self.callset_path,
            ),
        )

    def requires(self) -> luigi.Task:
        return WriteRemappedAndSubsettedCallsetTask(
            self.env,
            self.reference_genome,
            self.dataset_type,
            self.hail_temp_dir,
            self.callset_path,
            self.project_guid,
            self.project_remap_path,
            self.project_pedigree_path,
            self.ignore_missing_samples,
        )

    def initialize_table(self) -> hl.Table:
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                **key_type,
                filters=hl.tset(hl.tstr),
                entries=hl.tarray(self.dataset_type.genotype_entries_type),
            ),
            key=key_type.fields,
        )
        return ht.annotate_globals(
            sample_ids=hl.empty_array(hl.tstr),
            updates=hl.empty_set(hl.tstr),
        )

    def update(self, ht: hl.Table) -> hl.Table:
        # Filter out the samples that we're now loading from the current ht.
        callset_mt = hl.read_matrix_table(self.input().path)
        ht = deglobalize_sample_ids(ht)
        ht = remove_callset_sample_ids(ht, callset_mt.cols())

        # Merge the callset entries with the current ht entries
        callset_ht = callset_mt.select_rows(
            filters=callset_mt.filters,
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
        ht = union_entries_hts(ht, callset_ht)
        ht = globalize_sample_ids(ht)
        ht = ht.naive_coalesce(1)
        return ht.annotate_globals(
            updates=ht.updates.add(self.callset_path),
        )
