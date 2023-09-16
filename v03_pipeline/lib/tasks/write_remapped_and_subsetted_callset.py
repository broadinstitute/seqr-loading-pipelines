from __future__ import annotations

import hail as hl
import luigi

from v03_pipeline.lib.misc.io import does_file_exist, import_pedigree, import_remap
from v03_pipeline.lib.misc.sample_ids import remap_sample_ids, subset_samples
from v03_pipeline.lib.model import Env
from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, RawFileTask
from v03_pipeline.lib.tasks.write_imported_callset import WriteImportedCallsetTask
from v03_pipeline.lib.tasks.write_relatedness_check_table import (
    WriteRelatednessCheckTableTask,
)
from v03_pipeline.lib.tasks.write_sex_check_table import WriteSexCheckTableTask


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

    def requires(self) -> list[luigi.Task]:
        requirements = [
            WriteImportedCallsetTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                self.callset_path,
                # NB: filters_path is explicitly passed as None here
                # to avoid carrying it throughout the rest of the pipeline.
                # Only the primary import task itself should be aware of it.
                None,
                self.validate,
            ),
            RawFileTask(self.project_pedigree_path),
        ]
        if Env.RUN_SEX_AND_RELATEDNESS:
            requirements = [
                *requirements,
                WriteSexCheckTableTask(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                    self.callset_path,
                ),
                WriteRelatednessCheckTableTask(
                    self.reference_genome,
                    self.dataset_type,
                    self.sample_type,
                    self.callset_path,
                ),
            ]
        return requirements

    def create_table(self) -> hl.MatrixTable:
        callset_mt = hl.read_matrix_table(self.input()[0].path)
        callset_samples = set(callset_mt.cols().s.collect())
        pedigree_ht = import_pedigree(self.project_pedigree_path)

        # Remap, but only if the remap file is present!
        if does_file_exist(self.project_remap_path):
            project_remap_ht = import_remap(self.project_remap_path)
            callset_mt = remap_sample_ids(
                callset_mt,
                project_remap_ht,
                self.ignore_missing_samples_when_remapping,
            )
            if Env.RUN_SEX_AND_RELATEDNESS:
                remap_lookup = hl.dict(
                    {r.s: r.seqr_id for r in project_remap_ht.collect()},
                )
                sex_check_ht = hl.read_table(self.input()[1].path)
                sex_check_ht = sex_check_ht.transmute(
                    s=remap_lookup.get(sex_check_ht.s, sex_check_ht.s),
                )
                relatedness_check_ht = hl.read_table(self.input()[2].path)
                relatedness_check_ht = relatedness_check_ht.transmute(
                    i=remap_lookup.get(relatedness_check_ht.i, relatedness_check_ht.i),
                    j=remap_lookup.get(relatedness_check_ht.j, relatedness_check_ht.j),
                )

        families = parse_pedigree_ht_to_families(pedigree_ht)
        families_failed_missing_samples = set()
        for family in families:
            if len(family.sample_lineage.keys() - callset_samples) > 0:
                families_failed_missing_samples.add(family)

            if Env.RUN_SEX_AND_RELATEDNESS:
                pass

        callset_mt = subset_samples(
            callset_mt,
            callset_samples_ht,
            self.ignore_missing_samples_when_subsetting,
        )
        return callset_mt
