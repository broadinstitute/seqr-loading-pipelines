from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.sample_entries import (
    filter_callset_entries,
    globalize_sample_ids,
    join_entries_hts,
)
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class UpdateProjectTableTask(BaseUpdateTask):
    n_partitions = 10
    callset_path = luigi.Parameter()
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    is_new_gcnv_joint_call = luigi.BoolParameter(
        default=False,
        description='Is this a fully joint-called callset.',
    )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            project_table_path(
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
            self.reference_genome,
            self.dataset_type,
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
                # NB: entries is missing here because it is untyped
                # until we read the type off of the first callset aggregation.
            ),
            key=key_type.fields,
            globals=hl.Struct(
                sample_ids=hl.empty_array(hl.tstr),
                updates=hl.empty_set(hl.tstr),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input().path)
        callset_ht = callset_mt.select_rows(
            filters=callset_mt.filters.difference(self.dataset_type.excluded_filters),
            entries=hl.sorted(
                hl.agg.collect(
                    hl.Struct(
                        s=callset_mt.s,
                        **get_fields(
                            callset_mt,
                            self.dataset_type.genotype_entry_annotation_fns,
                            **self.param_kwargs,
                        ),
                    ),
                ),
                key=lambda e: e.s,
            ),
        ).rows()
        callset_ht = callset_ht.filter(
            callset_ht.entries.any(self.dataset_type.sample_entries_filter_fn),
        )
        callset_ht = globalize_sample_ids(callset_ht)
        # HACK: steal the type from callset_ht when ht is empty.
        # This was the least gross way
        if 'entries' not in ht.row_value:
            ht = ht.annotate(
                entries=hl.empty_array(callset_ht.entries.dtype.element_type),
            )
        ht = filter_callset_entries(ht, callset_mt.cols())
        ht = join_entries_hts(ht, callset_ht)
        return ht.annotate_globals(
            updates=ht.updates.add(self.callset_path),
        )
