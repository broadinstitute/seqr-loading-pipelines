from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.io import import_pedigree
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht_to_families
from v03_pipeline.lib.misc.sample_entries import globalize_sample_ids
from v03_pipeline.lib.misc.sample_ids import subset_samples
from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class WriteFamilyTableTask(BaseWriteTask):
    n_partitions = 2
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
    family_guid = luigi.Parameter()
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
            family_table_path(
                self.reference_genome,
                self.dataset_type,
                self.family_guid,
            ),
        )

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.read_table(self.output().path).updates.contains(self.callset_path),
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

    def create_table(self) -> hl.Table:
        pedigree_ht = import_pedigree(self.project_pedigree_path)
        callset_mt = hl.read_matrix_table(self.input().path)
        families = parse_pedigree_ht_to_families(pedigree_ht)
        callset_mt = subset_samples(
            callset_mt,
            hl.Table.parallelize(
                [
                    {'s': sample_id}
                    for family in families
                    if family.family_guid == self.family_guid
                    for sample_id in family.samples
                ],
                hl.tstruct(s=hl.dtype('str')),
                key='s',
            ),
            False,
        )
        ht = callset_mt.select_rows(
            filters=callset_mt.filters.difference(self.dataset_type.excluded_filters),
            entries=hl.sorted(
                hl.agg.collect(
                    hl.struct(
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
        ht = ht.filter(ht.entries.any(self.dataset_type.sample_entries_filter_fn))
        ht = globalize_sample_ids(ht)
        return ht.select_globals(
            updates={self.callset_path},
        )
