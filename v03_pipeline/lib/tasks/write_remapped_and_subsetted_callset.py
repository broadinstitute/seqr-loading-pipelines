from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.misc.io import does_file_exist, import_pedigree, import_remap
from v03_pipeline.lib.misc.pedigree import families_to_include, samples_to_include
from v03_pipeline.lib.misc.sample_ids import remap_sample_ids, subset_samples
from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import (
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
    RawFileTask,
)
from v03_pipeline.lib.tasks.write_imported_callset import WriteImportedCallsetTask


class WriteRemappedAndSubsettedCallsetTask(BaseWriteTask):
    n_partitions = 100
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
            remapped_and_subsetted_callset_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
                self.project_guid,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def requires(self) -> list[luigi.Task]:
        return [
            WriteImportedCallsetTask(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
                self.validate,
            ),
            RawFileTask(self.project_pedigree_path),
        ]

    def create_table(self) -> hl.MatrixTable:
        callset_mt = hl.read_matrix_table(self.input()[0].path)

        # Remap, but only if the remap file is present!
        if does_file_exist(self.project_remap_path):
            project_remap_ht = import_remap(self.project_remap_path)
            callset_mt = remap_sample_ids(
                callset_mt,
                project_remap_ht,
                self.ignore_missing_samples_when_remapping,
            )

        pedigree_ht = import_pedigree(self.project_pedigree_path)
        families_to_include_ht = families_to_include(pedigree_ht, callset_mt.cols())
        sample_subset_ht = samples_to_include(pedigree_ht, families_to_include_ht)
        callset_mt = subset_samples(
            callset_mt,
            sample_subset_ht,
            self.ignore_missing_samples_when_subsetting,
        )
        return callset_mt.annotate_globals(
            family_guids=sorted(families_to_include_ht.family_guid.collect()),
        )
