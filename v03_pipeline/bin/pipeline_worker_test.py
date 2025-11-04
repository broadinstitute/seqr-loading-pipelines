import json
import os
from unittest.mock import patch

import luigi

from v03_pipeline.bin.pipeline_worker import process_queue
from v03_pipeline.lib.core import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import (
    loading_pipeline_deadletter_queue_dir,
    loading_pipeline_queue_dir,
)
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
    @patch('v03_pipeline.lib.misc.slack._safe_post_to_slack')
    @patch('v03_pipeline.api.request_handlers.WriteSuccessFileTask')
    @patch('v03_pipeline.bin.pipeline_worker.logger')
    def test_process_queue(
        self,
        mock_logger,
        mock_write_success_file_task,
        mock_safe_post_to_slack,
    ):
        raw_request = {
            'request_type': 'LoadingPipelineRequest',
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
            ':white_check_mark: Pipeline Runner Request Success! :white_check_mark:\nRun ID: 20250916-200704-123456\n```{\n    "attempt_id": 0,\n    "callset_path": "v03_pipeline/var/test/callsets/1kg_30variants.vcf",\n    "dataset_type": "SNV_INDEL",\n    "project_guids": [\n        "project_a"\n    ],\n    "reference_genome": "GRCh38",\n    "request_type": "LoadingPipelineRequest",\n    "sample_type": "WGS",\n    "skip_check_sex_and_relatedness": false,\n    "skip_expect_tdr_metrics": false,\n    "skip_validation": false\n}```',
        )

    @patch('v03_pipeline.lib.misc.slack._safe_post_to_slack')
    @patch('v03_pipeline.api.request_handlers.WriteSuccessFileTask')
    @patch('v03_pipeline.bin.pipeline_worker.logger')
    def test_process_failure(
        self,
        mock_logger,
        mock_write_success_file_task,
        mock_safe_post_to_slack,
    ):
        raw_request = {
            'request_type': 'LoadingPipelineRequest',
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
        process_queue(local_scheduler=True)
        process_queue(local_scheduler=True)
        mock_safe_post_to_slack.assert_called_once_with(
            ':failed: Pipeline Runner Request Failed :failed:\nRun ID: 20250918-200704-123456\n```{\n    "attempt_id": 2,\n    "callset_path": "v03_pipeline/var/test/callsets/1kg_30variants.vcf",\n    "dataset_type": "SNV_INDEL",\n    "project_guids": [\n        "project_a"\n    ],\n    "reference_genome": "GRCh38",\n    "request_type": "LoadingPipelineRequest",\n    "sample_type": "WGS",\n    "skip_check_sex_and_relatedness": false,\n    "skip_expect_tdr_metrics": false,\n    "skip_validation": false\n}```\nReason: there were failed tasks',
        )
        self.assertEqual(len(os.listdir(loading_pipeline_queue_dir())), 0)
        with open(
            os.path.join(
                loading_pipeline_deadletter_queue_dir(),
                'request_20250918-200704-123456.json',
            ),
        ) as f:
            r = json.load(f)
            self.assertEqual(r['request_type'], 'LoadingPipelineRequest')
            self.assertEqual(r['attempt_id'], 2)
