import hail as hl
import luigi
import luigi.util

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.misc.family_loading_failures import (
    get_families_failed_missing_samples,
    get_families_failed_relatedness_check,
    get_families_failed_sex_check,
)
from v03_pipeline.lib.misc.io import (
    does_file_exist,
    import_pedigree,
    import_remap,
    remap_pedigree_hash,
)
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht_to_families
from v03_pipeline.lib.misc.sample_ids import remap_sample_ids, subset_samples
from v03_pipeline.lib.model.environment import Env
from v03_pipeline.lib.paths import remapped_and_subsetted_callset_path
from v03_pipeline.lib.tasks.base.base_loading_run_params import BaseLoadingRunParams
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, RawFileTask
from v03_pipeline.lib.tasks.validate_callset import ValidateCallsetTask
from v03_pipeline.lib.tasks.write_relatedness_check_table import (
    WriteRelatednessCheckTableTask,
)
from v03_pipeline.lib.tasks.write_sex_check_table import WriteSexCheckTableTask

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingRunParams)
class WriteRemappedAndSubsettedCallsetTask(BaseWriteTask):
    project_guid = luigi.Parameter()
    project_remap_path = luigi.Parameter()
    project_pedigree_path = luigi.Parameter()

    def complete(self) -> luigi.Target:
        return (
            not self.force
            and super().complete()
            and hl.eval(
                hl.read_table(self.output().path).globals.remap_pedigree_hash
                == remap_pedigree_hash(
                    self.project_remap_path, self.project_pedigree_path,
                ),
            )
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
            self.clone(ValidateCallsetTask, force=False),
            RawFileTask(self.project_pedigree_path),
        ]
        if (
            Env.CHECK_SEX_AND_RELATEDNESS
            and not self.skip_check_sex_and_relatedness
            and self.dataset_type.check_sex_and_relatedness
        ):
            requirements = [
                *requirements,
                self.clone(WriteRelatednessCheckTableTask),
                self.clone(WriteSexCheckTableTask),
            ]
        return requirements

    def create_table(self) -> hl.MatrixTable:
        callset_mt = hl.read_matrix_table(self.input()[0].path)
        pedigree_ht = import_pedigree(self.input()[1].path)

        # Remap, but only if the remap file is present!
        remap_lookup = hl.empty_dict(hl.tstr, hl.tstr)
        if does_file_exist(self.project_remap_path):
            project_remap_ht = import_remap(self.project_remap_path)
            callset_mt = remap_sample_ids(
                callset_mt,
                project_remap_ht,
                self.ignore_missing_samples_when_remapping,
            )
            remap_lookup = hl.dict(
                {r.s: r.seqr_id for r in project_remap_ht.collect()},
            )

        families = parse_pedigree_ht_to_families(pedigree_ht)
        families_failed_missing_samples = get_families_failed_missing_samples(
            callset_mt,
            families,
        )
        families_failed_relatedness_check = {}
        families_failed_sex_check = {}
        if (
            Env.CHECK_SEX_AND_RELATEDNESS
            and not self.skip_check_sex_and_relatedness
            and self.dataset_type.check_sex_and_relatedness
        ):
            relatedness_check_ht = hl.read_table(self.input()[2].path)
            sex_check_ht = hl.read_table(self.input()[3].path)
            families_failed_relatedness_check = get_families_failed_relatedness_check(
                families - families_failed_missing_samples.keys(),
                relatedness_check_ht,
                remap_lookup,
            )
            families_failed_sex_check = get_families_failed_sex_check(
                families
                - families_failed_missing_samples.keys()
                - families_failed_relatedness_check.keys(),
                sex_check_ht,
                remap_lookup,
            )

        loadable_families = (
            families
            - families_failed_missing_samples.keys()
            - families_failed_relatedness_check.keys()
            - families_failed_sex_check.keys()
        )
        if not len(loadable_families):
            msg = (
                f'families_failed_missing_samples: {families_failed_missing_samples}\n'
                f'families_failed_relatedness_check: {families_failed_relatedness_check}\n'
                f'families_failed_sex_check: {families_failed_sex_check}'
            )
            logger.info(
                msg,
            )
            msg = 'All families failed checks'
            raise RuntimeError(msg)

        mt = subset_samples(
            callset_mt,
            hl.Table.parallelize(
                [
                    {'s': sample_id}
                    for family in loadable_families
                    for sample_id in family.samples
                ],
                hl.tstruct(s=hl.dtype('str')),
                key='s',
            ),
        )
        # Drop additional fields imported onto the intermediate callsets but
        # not used when creating the downstream optimized tables.
        for field in mt.row_value:
            if field not in self.dataset_type.row_fields:
                mt = mt.drop(field)
        return mt.select_globals(
            remap_pedigree_hash=remap_pedigree_hash(
                self.project_remap_path,
                self.project_pedigree_path,
            ),
            family_samples=(
                {
                    f.family_guid: sorted(f.samples.keys())
                    for f in loadable_families
                    or hl.empty_dict(hl.tstr, hl.tarray(hl.tstr))
                }
            ),
            failed_family_samples=hl.Struct(
                missing_samples=(
                    {
                        f.family_guid: {
                            'samples': sorted(f.samples.keys()),
                            'reasons': reasons,
                        }
                        for f, reasons in families_failed_missing_samples.items()
                    }
                    or hl.empty_dict(hl.tstr, hl.tdict(hl.tstr, hl.tarray(hl.tstr)))
                ),
                relatedness_check=(
                    {
                        f.family_guid: {
                            'samples': sorted(f.samples.keys()),
                            'reasons': reasons,
                        }
                        for f, reasons in families_failed_relatedness_check.items()
                    }
                    or hl.empty_dict(hl.tstr, hl.tdict(hl.tstr, hl.tarray(hl.tstr)))
                ),
                sex_check=(
                    {
                        f.family_guid: {
                            'samples': sorted(f.samples.keys()),
                            'reasons': reasons,
                        }
                        for f, reasons in families_failed_sex_check.items()
                    }
                    or hl.empty_dict(hl.tstr, hl.tdict(hl.tstr, hl.tarray(hl.tstr)))
                ),
            ),
        )
