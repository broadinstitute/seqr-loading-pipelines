from __future__ import annotations

from typing import TYPE_CHECKING

import luigi

from v03_pipeline.lib.misc.io import import_callset
from v03_pipeline.lib.paths import imported_callset_path
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import (
    CallsetTask,
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
)

if TYPE_CHECKING:
    import hail as hl


class WriteImportedCallsetTask(BaseWriteTask):
    n_partitions = 500
    callset_path = luigi.Parameter()
    filters_path = luigi.OptionalParameter(
        default=None,
        description='Path to part two outputs from callset (VCF shards containing filter information)',
    )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            imported_callset_path(
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

    def create_table(self) -> hl.MatrixTable:
        return import_callset(
            self.callset_path,
            self.reference_genome,
            self.dataset_type,
            self.sample_type,
            self.filters_path,
        )
