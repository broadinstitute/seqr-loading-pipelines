import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.family_entries import (
    compute_callset_family_entries_ht,
    join_family_entries_hts,
    splice_new_callset_family_guids,
)
from v03_pipeline.lib.paths import project_table_path
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class UpdateProjectTableTask(BaseUpdateTask):
    callset_path = luigi.Parameter()
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples_when_subsetting = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    ignore_missing_samples_when_remapping = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    validate = luigi.BoolParameter(
        default=True,
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
        return super().complete() and hl.eval(
            hl.read_table(self.output().path).updates.contains(
                self.callset_path,
            ),
        )

    def requires(self) -> luigi.Task:
        return WriteRemappedAndSubsettedCallsetTask(
            self.reference_genome,
            self.dataset_type,
            self.sample_type,
            self.callset_path,
            self.project_guid,
            self.project_remap_path,
            self.project_pedigree_path,
            self.ignore_missing_samples_when_subsetting,
            self.ignore_missing_samples_when_remapping,
            self.validate,
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
                family_guids=hl.empty_array(hl.tstr),
                family_samples=hl.empty_dict(hl.tstr, hl.tarray(hl.tstr)),
                updates=hl.empty_set(hl.tstr),
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
                family_entries=hl.empty_array(
                    hl.tarray(callset_ht.family_entries.dtype.element_type),
                ),
            )
        ht = splice_new_callset_family_guids(ht, callset_mt.family_samples.collect()[0].keys())
        ht = join_family_entries_hts(ht, callset_ht)
        return ht.select_globals(
            family_guids=ht.family_guids,
            family_samples=ht.family_samples,
            sample_type=self.sample_type.value,
            updates=ht.updates.add(self.callset_path),
        )
