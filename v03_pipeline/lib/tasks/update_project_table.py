import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.family_entries import (
    compute_callset_family_entries_ht,
    join_family_entries_hts,
    remove_family_guids,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_update_project_table import (
    BaseUpdateProjectTableTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class UpdateProjectTableTask(BaseUpdateProjectTableTask):
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()

    def complete(self) -> bool:
        return (
            not self.force
            and super().complete()
            and hl.eval(
                hl.read_table(self.output().path).updates.contains(
                    self.callset_path,
                ),
            )
        )

    def requires(self) -> luigi.Task:
        return self.clone(WriteRemappedAndSubsettedCallsetTask, force=False)

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
            updates=ht.updates.add(self.callset_path),
        )
