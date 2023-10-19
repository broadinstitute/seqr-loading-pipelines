from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.misc.sample_lookup import (
    compute_callset_sample_lookup_ht,
    filter_callset_sample_ids,
    join_sample_lookup_hts,
)
from v03_pipeline.lib.paths import sample_lookup_table_path
from v03_pipeline.lib.tasks.base.base_update_task import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class UpdateSampleLookupTableTask(BaseUpdateTask):
    callset_path = luigi.Parameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
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
            sample_lookup_table_path(
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        return super().complete() and hl.eval(
            hl.bind(
                lambda updates: hl.all(
                    [
                        updates.contains(
                            hl.Struct(
                                callset=self.callset_path,
                                project_guid=project_guid,
                            ),
                        )
                        for project_guid in self.project_guids
                    ],
                ),
                hl.read_table(self.output().path).updates,
            ),
        )

    def requires(self) -> luigi.Task:
        return [
            WriteRemappedAndSubsettedCallsetTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.callset_path,
                project_guid,
                project_remap_path,
                project_pedigree_path,
                self.ignore_missing_samples_when_subsetting,
                self.ignore_missing_samples_when_remapping,
                self.validate,
            )
            for (project_guid, project_remap_path, project_pedigree_path) in zip(
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
            )
        ]

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
        for i, project_guid in enumerate(self.project_guids):
            callset_mt = hl.read_matrix_table(self.input()[i].path)
            ht = filter_callset_sample_ids(
                self.dataset_type,
                ht,
                callset_mt.cols(),
                project_guid,
            )
            callset_sample_lookup_ht = compute_callset_sample_lookup_ht(
                self.dataset_type,
                callset_mt,
            )
            ht = join_sample_lookup_hts(
                self.dataset_type,
                ht,
                callset_sample_lookup_ht,
                project_guid,
            )
            ht = ht.select_globals(
                updates=ht.updates.add(
                    hl.Struct(callset=self.callset_path, project_guid=project_guid),
                ),
            )
        return ht
