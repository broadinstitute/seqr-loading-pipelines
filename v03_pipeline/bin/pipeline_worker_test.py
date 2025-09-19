import json
import os
from unittest.mock import patch

import luigi

from v03_pipeline.bin.pipeline_worker import process_queue
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import loading_pipeline_queue_dir
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'


class MyFailingTask(luigi.Task):
    def run(self):
        msg = 'This task is designed to fail!'
        raise ValueError(msg)

    def output(self):
        return luigi.LocalTarget('output.txt')


class PipelineWorkerTest(MockedDatarootTestCase):
    @patch('v03_pipeline.lib.paths.LOCAL_DISK_MOUNT_PATH', './var/seqr')
    @patch('v03_pipeline.lib.misc.slack._safe_post_to_slack')
    @patch('v03_pipeline.bin.pipeline_worker.WriteSuccessFileTask')
    @patch('v03_pipeline.bin.pipeline_worker.logger')
    def test_process_queue(
        self,
        mock_logger,
        mock_write_success_file_task,
        mock_safe_post_to_slack,
    ):
        raw_request = {
            'callset_path': TEST_VCF,
            'projects_to_run': ['project_a'],
            'sample_type': SampleType.WGS.value,
            'reference_genome': ReferenceGenome.GRCh38.value,
            'dataset_type': DatasetType.SNV_INDEL.value,
        }
        mock_write_success_file_task.return_value = MockCompleteTask()
        os.makedirs(
            loading_pipeline_queue_dir(),
            exist_ok=True,
        )
        with open(
            os.path.join(
                loading_pipeline_queue_dir(),
                'request_20250916-200704-123456.json',
            ),
            'w',
        ) as f:
            json.dump(raw_request, f)
        process_queue(local_scheduler=True)
        mock_safe_post_to_slack.assert_called_once_with(
            ':white_check_mark: Pipeline Run Success! Kicking off ClickHouse Load! :white_check_mark:\nRun ID: 20250916-200704-123456\nCallset Path: v03_pipeline/var/test/callsets/1kg_30variants.vcf\nProject Guids: project_a\nReference Genome: GRCh38\nDataset Type: SNV_INDEL\nSample Type: WGS\nSkip Validation: False\nSkip Sex & Relatedness: False\nSkip Expect TDR Metrics: False',
        )

    @patch('v03_pipeline.lib.paths.LOCAL_DISK_MOUNT_PATH', './var/seqr')
    @patch('v03_pipeline.lib.misc.slack._safe_post_to_slack')
    @patch('v03_pipeline.bin.pipeline_worker.WriteSuccessFileTask')
    @patch('v03_pipeline.bin.pipeline_worker.logger')
    def test_process_failure(
        self,
        mock_logger,
        mock_write_success_file_task,
        mock_safe_post_to_slack,
    ):
        raw_request = {
            'callset_path': TEST_VCF,
            'projects_to_run': ['project_a'],
            'sample_type': SampleType.WGS.value,
            'reference_genome': ReferenceGenome.GRCh38.value,
            'dataset_type': DatasetType.SNV_INDEL.value,
        }
        mock_write_success_file_task.return_value = MyFailingTask()
        os.makedirs(
            loading_pipeline_queue_dir(),
            exist_ok=True,
        )
        with open(
            os.path.join(
                loading_pipeline_queue_dir(),
                'request_20250918-200704-123456.json',
            ),
            'w',
        ) as f:
            json.dump(raw_request, f)
        process_queue(local_scheduler=True)
        mock_safe_post_to_slack.assert_called_once_with(
            ':failed: Pipeline Run Failed. :failed:\nRun ID: 20250918-200704-123456\nCallset Path: v03_pipeline/var/test/callsets/1kg_30variants.vcf\nProject Guids: project_a\nReference Genome: GRCh38\nDataset Type: SNV_INDEL\nSample Type: WGS\nSkip Validation: False\nSkip Sex & Relatedness: False\nSkip Expect TDR Metrics: False\nReason: there were failed tasks',
        )
