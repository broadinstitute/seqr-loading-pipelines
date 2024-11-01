import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.family_entries import (
    compute_callset_family_entries_ht,
    join_family_entries_hts,
    remove_family_guids,
)
from v03_pipeline.lib.misc.io import remap_pedigree_hash
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update import (
    BaseUpdateTask,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class UpdateProjectTableTask(BaseUpdateTask):
    project_i = luigi.IntParameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            project_table_path(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.project_guids[self.project_i],
            ),
        )

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(self.output().path).updates.contains(
                hl.Struct(
                    callset=self.callset_path,
                    remap_pedigree_hash=remap_pedigree_hash(
                        self.project_remap_paths[self.project_i],
                        self.project_pedigree_paths[self.project_i],
                    ),
                ),
            ),
        )

    def requires(self) -> luigi.Task:
        return self.clone(WriteRemappedAndSubsettedCallsetTask)

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
                family_guids=hl.empty_array(hl.tstr),
                family_samples=hl.empty_dict(hl.tstr, hl.tarray(hl.tstr)),
                updates=hl.empty_set(
                    hl.tstruct(callset=hl.tstr, remap_pedigree_hash=hl.tint32),
                ),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input().path)
        callset_ht = compute_callset_family_entries_ht(
            self.dataset_type,
            callset_mt,
            get_fields(
                callset_mt,
                self.dataset_type.genotype_entry_annotation_fns,
                **self.param_kwargs,
            ),
        )
        # HACK: steal the type from callset_ht when ht is empty.
        # This was the least gross way
        if 'family_entries' not in ht.row_value:
            ht = ht.annotate(
                family_entries=hl.missing(callset_ht.family_entries.dtype),
            )
        ht = remove_family_guids(
            ht,
            callset_mt.index_globals().family_samples.key_set(),
        )
        ht = join_family_entries_hts(ht, callset_ht)
        return ht.select_globals(
            family_guids=ht.family_guids,
            family_samples=ht.family_samples,
            sample_type=self.sample_type.value,
            updates=ht.updates.add(
                hl.Struct(
                    callset=self.callset_path,
                    remap_pedigree_hash=remap_pedigree_hash(
                        self.project_remap_paths[self.project_i],
                        self.project_pedigree_paths[self.project_i],
                    ),
                ),
            ),
        )
