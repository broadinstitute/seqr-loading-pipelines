import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.family_entries import compute_callset_family_entries_ht
from v03_pipeline.lib.misc.io import import_pedigree
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht_to_families
from v03_pipeline.lib.misc.sample_ids import subset_samples
from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class WriteFamilyTableTask(BaseWriteTask):
    callset_path = luigi.Parameter()
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples_when_subsetting = luigi.BoolParameter(
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    ignore_missing_samples_when_remapping = luigi.BoolParameter(
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    validate = luigi.BoolParameter(
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    force = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    is_new_gcnv_joint_call = luigi.BoolParameter(
        description='Is this a fully joint-called callset.',
    )
    family_guid = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            family_table_path(
                self.reference_genome,
                self.dataset_type,
                self.family_guid,
            ),
        )

    def complete(self) -> bool:
        return (
            not self.force
            and super().complete()
            and hl.eval(
                hl.read_table(self.output().path).updates.contains(self.callset_path),
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
            self.ignore_missing_samples_when_subsetting,
            self.ignore_missing_samples_when_remapping,
            self.validate,
            False,
        )

    def create_table(self) -> hl.Table:
        callset_mt = hl.read_matrix_table(self.input().path)
        pedigree_ht = import_pedigree(self.project_pedigree_path)
        families = parse_pedigree_ht_to_families(pedigree_ht)
        family = next(
            iter(
                family for family in families if family.family_guid == self.family_guid
            ),
        )
        callset_mt = subset_samples(
            callset_mt,
            hl.Table.parallelize(
                [{'s': sample_id} for sample_id in family.samples],
                hl.tstruct(s=hl.dtype('str')),
                key='s',
            ),
            False,
        )
        ht = compute_callset_family_entries_ht(
            self.dataset_type,
            callset_mt,
            get_fields(
                callset_mt,
                self.dataset_type.genotype_entry_annotation_fns,
                **self.param_kwargs,
            ),
        )
        ht = ht.transmute(
            entries=hl.flatten(ht.family_entries),
        )
        return ht.select_globals(
            sample_ids=ht.family_samples[self.family_guid],
            sample_type=self.sample_type.value,
            updates={self.callset_path},
        )
