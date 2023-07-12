from __future__ import annotations

import luigi

from v03_pipeline.lib.misc.io import import_callset, write
from v03_pipeline.lib.paths import imported_callset_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import (
    CallsetTask,
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
)


class WriteImportedCallsetTask(BasePipelineTask):
    callset_path = luigi.Parameter()

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            imported_callset_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def requires(self) -> list[luigi.Task]:
        return [
            CallsetTask(self.callset_path),
        ]

    def run(self) -> None:
        self.init_hail()
        callset_mt = import_callset(
            self.callset_path,
            self.env,
            self.reference_genome,
            self.dataset_type,
        )
        write(self.env, callset_mt, self.output().path, False)
