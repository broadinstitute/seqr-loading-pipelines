from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.misc.sample_lookup import (
    compute_sample_lookup_ht,
    remove_callset_sample_ids,
    union_sample_lookup_hts,
)
from v03_pipeline.lib.paths import sample_lookup_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallset,
)


class UpdateSampleLookupTableTask(BasePipelineTask):
    callset_path = luigi.Parameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    ignore_missing_samples = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            sample_lookup_table_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists() and hl.eval(
            hl.all(
                [
                    hl.read_table(self.output().path).updates.contains(
                        hl.Struct(callset=self.callset_path, project_guid=project_guid),
                    )
                    for project_guid in self.project_guids
                ],
            ),
        )

    def requires(self) -> luigi.Task:
        return [
            WriteRemappedAndSubsettedCallset(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.hail_temp_dir,
                self.callset_path,
                project_guid,
                project_remap_path,
                project_pedigree_path,
                self.ignore_missing_samples,
            )
            for (project_guid, project_remap_path, project_pedigree_path) in zip(
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
            )
        ]

    def initialize_table(self) -> hl.Table:
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                **key_type,
                ref_samples=hl.tdict(hl.tstr, hl.tset(hl.tstr)),
                het_samples=hl.tdict(hl.tstr, hl.tset(hl.tstr)),
                hom_samples=hl.tdict(hl.tstr, hl.tset(hl.tstr)),
            ),
            key=key_type.fields,
        )
        return ht.annotate_globals(
            updates=hl.empty_set(hl.tstruct(callset=hl.tstr, project_guid=hl.tstr)),
        )

    def update(self, ht: hl.Table) -> hl.Table:
        for i, project_guid in enumerate(self.project_guids):
            callset_mt = hl.read_matrix_table(self.input()[i].path)
            ht = remove_callset_sample_ids(ht, callset_mt.cols(), project_guid)
            ht = union_sample_lookup_hts(
                ht,
                compute_sample_lookup_ht(callset_mt, project_guid),
                project_guid,
            )
            ht = ht.annotate_globals(
                updates=ht.updates.add(
                    hl.Struct(callset=self.callset_path, project_guid=project_guid),
                ),
            )
        return ht
