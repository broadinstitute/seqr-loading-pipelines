from __future__ import annotations

import luigi

from v03_pipeline.lib.misc.io import (
    import_callset,
    import_pedigree,
    import_remap,
    write,
)
from v03_pipeline.lib.misc.pedigree import samples_to_include
from v03_pipeline.lib.misc.sample_ids import remap_sample_ids, subset_samples
from v03_pipeline.lib.model import SampleFileType
from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import (
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
    RawFileTask,
    VCFFileTask,
)


class WriteRemappedAndSubsettedCallset(BasePipelineTask):
    callset_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples = luigi.BoolParameter(default=False)

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            remapped_and_subsetted_callset_path(
                self.env,
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
                self.project_pedigree_path,
            ),
        )

    def complete(self) -> bool:
        return GCSorLocalFolderTarget(self.output().path).exists()

    def requires(self) -> list[luigi.Task]:
        return [
            VCFFileTask(self.callset_path)
            if self.dataset_type.sample_file_type == SampleFileType.VCF
            else RawFileTask(self.callset_path),
            RawFileTask(self.project_remap_path),
            RawFileTask(self.project_pedigree_path),
        ]

    def run(self) -> None:
        self.init_hail()
        # Import required files.
        callset_mt = import_callset(
            self.callset_path,
            self.env,
            self.reference_genome,
            self.dataset_type,
        )
        project_remap_ht = import_remap(self.project_remap_path)
        pedigree_ht = import_pedigree(self.project_pedigree_path)

        # Remap, then subset to samples of interest.
        callset_mt = remap_sample_ids(callset_mt, project_remap_ht)
        sample_subset_ht = samples_to_include(pedigree_ht, callset_mt.cols())
        callset_mt = subset_samples(
            callset_mt,
            sample_subset_ht,
            self.ignore_missing_samples,
        )
        write(self.env, callset_mt, self.output().path)
