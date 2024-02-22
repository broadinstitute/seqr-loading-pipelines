import hail as hl
import luigi

from v03_pipeline.lib.misc.family_lookup import (
    compute_callset_family_lookup_ht,
    remove_new_callset_family_guids,
    join_family_lookup_hts,
)
from v03_pipeline.lib.paths import family_lookup_table_path
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class UpdateFamilyLookupTableTask(BaseUpdateTask):
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

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            family_lookup_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(self.output().path).updates.contains(
                hl.Struct(
                    callset=self.callset_path,
                    project_guid=self.project_guid,
                ),
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
                **{
                    field: hl.tstruct()
                    for field in self.dataset_type.sample_lookup_table_fields_and_genotype_filter_fns
                },
            ),
            key=key_type.fields,
            globals=hl.Struct(
                updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input().path)
        ht = remove_new_callset_family_guids(
            ht,
            self.project_guid,
            list(callset_mt.family_samples.collect()[0].keys()),
        )
        callset_ht = compute_callset_family_lookup_ht(
            self.dataset_type,
            callset_mt,
            self.project_guid,
        )
        ht = join_sample_lookup_hts(
            ht,
            callset_ht,
        )
        return ht.select_globals(
            project_guids=ht.project_guids,
            project_families=ht.project_families,
            updates=ht.updates.add(
                hl.Struct(callset=self.callset_path, project_guid=self.project_guid),
            ),
        )
