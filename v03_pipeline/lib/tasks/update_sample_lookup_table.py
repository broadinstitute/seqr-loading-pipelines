from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.misc.io import import_callset, import_pedigree, import_remap
from v03_pipeline.lib.misc.pedigree import samples_to_include
from v03_pipeline.lib.misc.sample_ids import (
    remap_sample_ids,
    subset_samples_and_variants,
)
from v03_pipeline.lib.model import SampleFileType, SampleType
from v03_pipeline.lib.paths import sample_lookup_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import (
    GCSorLocalFolderTarget,
    GCSorLocalTarget,
    RawFileTask,
    VCFFileTask,
)


class UpdateSampleLookupTableTask(BasePipelineTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_path = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    dont_validate = luigi.BoolParameter(
        default=False,
        description='Disable checking whether the dataset matches the specified sample type and genome version',
    )
    ignore_missing_samples = luigi.BoolParameter(default=False)

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
            hl.read_table(self.output().path).updates.contains(
                (self.callset_path, self.project_pedigree_path),
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            VCFFileTask(self.callset_path)
            if self.dataset_type.sample_file_type == SampleFileType.VCF
            else RawFileTask(self.callset_path),
            RawFileTask(self.project_remap_path),
            RawFileTask(self.project_pedigree_path),
        ]

    def initialize_table(self) -> hl.Table:
        key_type = self.dataset_type.table_key_type(self.reference_genome)
        ht = hl.Table.parallelize(
            [],
            key_type,
            key=key_type.fields,
        )
        return ht.annotate_globals(
            updates=hl.empty_set(hl.ttuple(hl.tstr, hl.tstr)),
        )

    def update(self, ht: hl.Table) -> hl.Table:
        # Import required files.
        #callset_mt = import_callset(
        #    self.callset_path,
        #    self.reference_genome,
        #    self.dataset_type,
        #)
        #project_remap_ht = import_remap(self.project_remap_path)
        #pedigree_ht = import_pedigree(self.project_pedigree_path)
#
        ## Remap, then subset to samples & variants of interest.
        #callset_mt = remap_sample_ids(callset_mt, project_remap_ht)
        #sample_subset_ht = samples_to_include(pedigree_ht, callset_mt.cols())
        #callset_mt = subset_samples_and_variants(
        #    callset_mt,
        #    sample_subset_ht,
        #    self.ignore_missing_samples,
        #)
        #callset_mt.anti_join_rows(ht)
        return ht.annotate_globals(
            updates=ht.updates.add(
                (self.callset_path, self.project_pedigree_path),
            ),
        )
