import unittest
from unittest.mock import Mock, call, patch

import luigi

from v03_pipeline.lib.model import (
    DatasetType,
    Env,
    FeatureFlag,
    ReferenceGenome,
    SampleType,
)
from v03_pipeline.lib.tasks.dataproc.rsync_to_seqr_app_dirs import (
    RsyncToSeqrAppDirsTask,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask


class RsyncToSeqrAppDirsTaskTest(unittest.TestCase):
    @patch(
        'v03_pipeline.lib.tasks.dataproc.rsync_to_seqr_app_dirs.RunPipelineOnDataprocTask',
    )
    @patch('v03_pipeline.lib.tasks.dataproc.rsync_to_seqr_app_dirs.subprocess')
    def test_rsync_to_seqr_app_dirs_no_sync(
        self,
        mock_subprocess: Mock,
        mock_run_pipeline_task: Mock,
    ) -> None:
        mock_run_pipeline_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = RsyncToSeqrAppDirsTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='test_callset',
            project_guids=['R0113_test_project'],
            project_remap_paths=['test_remap'],
            project_pedigree_paths=['test_pedigree'],
            run_id='manual__2024-04-01',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        mock_subprocess.call.assert_not_called()

    @patch(
        'v03_pipeline.lib.tasks.dataproc.rsync_to_seqr_app_dirs.RunPipelineOnDataprocTask',
    )
    @patch('v03_pipeline.lib.tasks.dataproc.rsync_to_seqr_app_dirs.subprocess')
    @patch.object(Env, 'HAIL_SEARCH_DATA_DIR', 'gs://test-hail-search-dir')
    @patch.object(Env, 'REFERENCE_DATASETS_DIR', 'gs://test-reference-data-dir')
    @patch.object(
        Env,
        'SEQR_APP_HAIL_SEARCH_DATA_DIR',
        '/var/seqr/seqr-hail-search-data',
    )
    @patch.object(
        Env,
        'SEQR_APP_REFERENCE_DATASETS_DIR',
        '/var/seqr/seqr-reference-data',
    )
    @patch.object(
        FeatureFlag,
        'INCLUDE_PIPELINE_VERSION_IN_PREFIX',
        False,
    )
    def test_rsync_to_seqr_app_dirs_sync(
        self,
        mock_subprocess: Mock,
        mock_run_pipeline_task: Mock,
    ) -> None:
        mock_run_pipeline_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        task = RsyncToSeqrAppDirsTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='test_callset',
            project_guids=['R0113_test_project'],
            project_remap_paths=['test_remap'],
            project_pedigree_paths=['test_pedigree'],
            run_id='manual__2024-04-02',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
        mock_subprocess.call.assert_has_calls(
            [
                call(
                    [
                        '/bin/bash',
                        '-cx',
                        'mkdir -p /var/seqr/seqr-hail-search-data/GRCh38/SNV_INDEL && gsutil -qm rsync -rd -x .*runs.* gs://test-hail-search-dir/GRCh38/SNV_INDEL /var/seqr/seqr-hail-search-data/GRCh38/SNV_INDEL && sync /var/seqr/seqr-hail-search-data/GRCh38/SNV_INDEL',
                    ],
                ),
                call(
                    [
                        '/bin/bash',
                        '-cx',
                        'mkdir -p /var/seqr/seqr-reference-data/GRCh38/SNV_INDEL/high_af_variants.ht && gsutil -qm rsync -rd -x .*runs.* gs://test-reference-data-dir/GRCh38/SNV_INDEL/high_af_variants.ht /var/seqr/seqr-reference-data/GRCh38/SNV_INDEL/high_af_variants.ht && sync /var/seqr/seqr-reference-data/GRCh38/SNV_INDEL/high_af_variants.ht',
                    ],
                ),
                call(
                    [
                        '/bin/bash',
                        '-cx',
                        'mkdir -p /var/seqr/seqr-reference-data/GRCh38/SNV_INDEL/clinvar_path_variants.ht && gsutil -qm rsync -rd -x .*runs.* gs://test-reference-data-dir/GRCh38/SNV_INDEL/clinvar_path_variants.ht /var/seqr/seqr-reference-data/GRCh38/SNV_INDEL/clinvar_path_variants.ht && sync /var/seqr/seqr-reference-data/GRCh38/SNV_INDEL/clinvar_path_variants.ht',
                    ],
                ),
            ],
            any_order=True,
        )
