from __future__ import annotations

import json

import hail as hl
import luigi

from v03_pipeline.lib.paths import metadata_for_run_path
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class WriteMetadataForRunTask(BaseWriteTask):
    callset_path = luigi.Parameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    ignore_missing_samples = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    run_id = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            metadata_for_run_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalTarget(self.output().path).exists()

    def requires(self) -> luigi.Task:
        return [
            WriteRemappedAndSubsettedCallsetTask(
                self.reference_genome,
                self.dataset_type,
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

    def run(self) -> None:
        metadata_json = {'projects': {}, 'callset': self.callset_path}
        for project_guid, remapped_and_subsetted_callset in zip(
            self.project_guids,
            self.input(),
        ):
            callset_mt = hl.read_matrix_table(remapped_and_subsetted_callset.path)
            metadata_json['projects'][project_guid] = callset_mt.cols().s.collect()

        with self.output().open('w') as f:
            json.dump(metadata_json, f)
