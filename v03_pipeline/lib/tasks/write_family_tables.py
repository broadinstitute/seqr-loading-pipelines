from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.annotations.fields import get_fields
from v03_pipeline.lib.misc.io import import_pedigree, write
from v03_pipeline.lib.misc.pedigree import samples_to_include
from v03_pipeline.lib.misc.sample_entries import (
    filter_hom_ref_rows,
    globalize_sample_ids,
)
from v03_pipeline.lib.misc.sample_ids import subset_samples
from v03_pipeline.lib.model import AnnotationType
from v03_pipeline.lib.paths import family_table_path
from v03_pipeline.lib.tasks.base.base_pipeline_task import BasePipelineTask
from v03_pipeline.lib.tasks.files import GCSorLocalFolderTarget, GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class WriteFamilyTablesTask(BasePipelineTask):
    callset_path = luigi.Parameter()
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()
    ignore_missing_samples = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    family_guids = luigi.ListParameter()

    def output(self) -> list[luigi.Target]:
        return [
            GCSorLocalTarget(
                family_table_path(
                    self.env,
                    self.reference_genome,
                    self.dataset_type,
                    family_guid,
                ),
            )
            for family_guid in self.family_guids
        ]

    def complete(self) -> bool:
        return all(
            GCSorLocalFolderTarget(output.path).exists()
            and hl.eval(hl.read_table(output.path).updates.contains(self.callset_path))
            for output in self.output()
        )

    def requires(self) -> luigi.Task:
        return WriteRemappedAndSubsettedCallsetTask(
            self.env,
            self.reference_genome,
            self.dataset_type,
            self.hail_temp_dir,
            self.callset_path,
            self.project_guid,
            self.project_remap_path,
            self.project_pedigree_path,
            self.ignore_missing_samples,
        )

    def run(self) -> None:
        self.init_hail()
        pedigree_ht = import_pedigree(self.project_pedigree_path)
        callset_mt = hl.read_matrix_table(self.input().path)
        callset_family_guids = set(callset_mt.family_guids.collect()[0])
        for family_guid, target in zip(self.family_guids, self.output()):
            if family_guid not in callset_family_guids:
                msg = f'Family: {family_guid} was not complete in this callset'
                raise ValueError(msg)
            sample_subset_ht = samples_to_include(
                pedigree_ht,
                hl.Table.parallelize(
                    [
                        {'family_guid': family_guid},
                    ],
                    hl.tstruct(
                        family_guid=hl.dtype('str'),
                    ),
                    key='family_guid',
                ),
            )
            callset_mt = subset_samples(callset_mt, sample_subset_ht, False)
            ht = callset_mt.select_rows(
                filters=callset_mt.filters,
                entries=hl.sorted(
                    hl.agg.collect(
                        hl.struct(
                            s=callset_mt.s,
                            **get_fields(
                                callset_mt,
                                AnnotationType.GENOTYPE_ENTRIES,
                                **self.param_kwargs,
                            ),
                        ),
                    ),
                    key=lambda e: e.s,
                ),
            ).rows()
            ht = globalize_sample_ids(ht)
            ht = filter_hom_ref_rows(ht)
            ht = ht.naive_coalesce(1)
            ht = ht.annotate_globals(
                updates={self.callset_path},
            )
            write(self.env, ht, target.path, False)
