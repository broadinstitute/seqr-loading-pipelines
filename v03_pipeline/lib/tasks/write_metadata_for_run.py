import json

import hail as hl
import luigi

from v03_pipeline.lib.misc.callsets import callset_project_pairs
from v03_pipeline.lib.model import SampleType
from v03_pipeline.lib.paths import metadata_for_run_path
from v03_pipeline.lib.tasks.base.base_hail_table import BaseHailTableTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


class WriteMetadataForRunTask(BaseHailTableTask):
    sample_type = luigi.EnumParameter(enum=SampleType)
    callset_paths = luigi.ListParameter()
    project_guids = luigi.ListParameter()
    project_remap_paths = luigi.ListParameter()
    project_pedigree_paths = luigi.ListParameter()
    imputed_sex_paths = luigi.ListParameter(default=None)
    ignore_missing_samples_when_remapping = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    validate = luigi.BoolParameter(
        default=True,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    force = luigi.BoolParameter(
        default=False,
        parsing=luigi.BoolParameter.EXPLICIT_PARSING,
    )
    check_sex_and_relatedness = luigi.BoolParameter(
        default=True,
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

    def requires(self) -> list[luigi.Task]:
        return [
            WriteRemappedAndSubsettedCallsetTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                callset_path,
                project_guid,
                project_remap_path,
                project_pedigree_path,
                imputed_sex_path,
                self.ignore_missing_samples_when_remapping,
                self.validate,
                self.force,
                self.check_sex_and_relatedness,
            )
            for (
                callset_path,
                project_guid,
                project_remap_path,
                project_pedigree_path,
                imputed_sex_path,
            ) in callset_project_pairs(
                self.callset_paths,
                self.project_guids,
                self.project_remap_paths,
                self.project_pedigree_paths,
                self.imputed_sex_paths,
            )
        ]

    def run(self) -> None:
        metadata_json = {
            'callsets': self.callset_paths,
            'run_id': self.run_id,
            'sample_type': self.sample_type.value,
            'family_samples': {},
            'failed_family_samples': {
                'missing_samples': {},
                'relatedness_check': {},
                'sex_check': {},
            },
        }
        for remapped_and_subsetted_callset in self.input():
            callset_mt = hl.read_matrix_table(remapped_and_subsetted_callset.path)
            collected_globals = callset_mt.globals.collect()[0]
            metadata_json['family_samples'] = {
                **collected_globals['family_samples'],
                **metadata_json['family_samples'],
            }
            for key in ['missing_samples', 'relatedness_check', 'sex_check']:
                metadata_json['failed_family_samples'][key] = {
                    **collected_globals['failed_family_samples'][key],
                    **metadata_json['failed_family_samples'][key],
                }

        with self.output().open('w') as f:
            json.dump(metadata_json, f)
