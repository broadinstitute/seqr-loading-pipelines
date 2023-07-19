from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.sample_entries import (
    filter_callset_entries,
    filter_hom_ref_rows,
    globalize_sample_ids,
    join_entries_hts,
)
from v03_pipeline.lib.model import AnnotationType
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class UpdateProjectTableTask(BaseUpdateTask):
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
        return hl.Table.parallelize(
            [],
            hl.tstruct(
                **key_type,
                filters=hl.tset(hl.tstr),
                entries=hl.tarray(self.dataset_type.sample_entries_type),
            ),
            key=key_type.fields,
            globals=hl.Struct(
                sample_ids=hl.empty_array(hl.tstr),
                updates=hl.empty_set(hl.tstr),
            ),
        )

    def update(self, ht: hl.Table) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input().path)
        callset_ht = callset_mt.select_rows(
            filters=callset_mt.filters,
            entries=hl.sorted(
                hl.agg.collect(
                    hl.Struct(
                        s=callset_mt.s,
                        **get_fields(
                            callset_mt,
                            AnnotationType.GENOTYPE_ENTRIES,
                            **self.param_kwargs,
                        ),
                    ),
                ),
                key=lambda e: e.s,
            ),
        ).rows()
        callset_ht = globalize_sample_ids(callset_ht)
        callset_ht = filter_hom_ref_rows(callset_ht)
        ht = filter_callset_entries(ht, callset_mt.cols())
        ht = join_entries_hts(ht, callset_ht)
        return ht.annotate_globals(
            updates=ht.updates.add(self.callset_path),
        )
