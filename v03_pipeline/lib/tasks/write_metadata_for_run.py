import json

import hail as hl
import hailtop.fs as hfs
import luigi
import luigi.util

from v03_pipeline.lib.model import FeatureFlag
from v03_pipeline.lib.paths import (
    metadata_for_run_path,
    relatedness_check_tsv_path,
    sample_qc_json_path,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class WriteMetadataForRunTask(luigi.Task):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            metadata_for_run_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return [
            self.clone(
                WriteRemappedAndSubsettedCallsetTask,
                project_i=i,
            )
            for i in range(len(self.project_guids))
        ]

    def run(self) -> None:
        metadata_json = {
            'callsets': [self.callset_path],
            'run_id': self.run_id,
            'sample_type': self.sample_type.value,
            'project_guids': self.project_guids,
            'family_samples': {},
            'failed_family_samples': {
                'missing_samples': {},
                'relatedness_check': {},
                'sex_check': {},
                'ploidy_check': {},
            },
            'relatedness_check_file_path': relatedness_check_tsv_path(
                self.reference_genome,
                self.dataset_type,
                self.callset_path,
            ),
            'sample_qc': {},
        }
        sample_qc_loadable_samples = {}
        for remapped_and_subsetted_callset in self.input():
            callset_mt = hl.read_matrix_table(remapped_and_subsetted_callset.path)
            collected_globals = callset_mt.globals.collect()[0]
            metadata_json['family_samples'] = {
                **collected_globals['family_samples'],
                **metadata_json['family_samples'],
            }
            sample_qc_loadable_samples = {
                *{
                    sample
                    for family_samples in collected_globals['family_samples'].values()
                    for sample in family_samples
                },
                *sample_qc_loadable_samples,
            }
            for key in [
                'missing_samples',
                'relatedness_check',
                'sex_check',
                'ploidy_check',
            ]:
                metadata_json['failed_family_samples'][key] = {
                    **collected_globals['failed_family_samples'][key],
                    **metadata_json['failed_family_samples'][key],
                }
                sample_qc_loadable_samples = {
                    *{
                        sample
                        for meta in collected_globals['failed_family_samples'][
                            key
                        ].values()
                        for sample in meta['samples']
                    },
                    *sample_qc_loadable_samples,
                }
        if (
            FeatureFlag.EXPECT_TDR_METRICS
            and not self.skip_expect_tdr_metrics
            and self.dataset_type.expect_tdr_metrics(
                self.reference_genome,
            )
        ):
            with hfs.open(
                sample_qc_json_path(
                    self.reference_genome,
                    self.dataset_type,
                    self.callset_path,
                ),
            ) as f:
                metadata_json['sample_qc'] = {
                    k: v
                    for k, v in json.load(f).items()
                    if k in sample_qc_loadable_samples
                }
        with self.output().open('w') as f:
            json.dump(metadata_json, f)
