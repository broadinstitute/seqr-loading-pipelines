import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.family_entries import (
    compute_callset_family_entries_ht,
    join_family_entries_hts,
    remove_family_guids,
)
from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.tasks.base.base_update_project_table import (
    BaseUpdateProjectTableTask,
)
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class UpdateProjectTableTask(BaseUpdateProjectTableTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    imputed_sex_path = luigi.Parameter(default=None)
    ignore_missing_samples_when_remapping = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    validate = luigi.BoolParameter(
        default=True,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    force = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    is_new_gcnv_joint_call = luigi.BoolParameter(
        default=False,
        description='Is this a fully joint-called callset.',
    )

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
        return WriteRemappedAndSubsettedCallsetTask(
            self.reference_genome,
            self.dataset_type,
            self.sample_type,
            self.callset_path,
            self.project_guid,
            self.project_remap_path,
            self.project_pedigree_path,
            self.imputed_sex_path,
            self.ignore_missing_samples_when_remapping,
            self.validate,
            False,
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
            updates=ht.updates.add(self.callset_path),
        )
