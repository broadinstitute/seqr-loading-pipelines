import json
import os
from unittest.mock import patch

from v03_pipeline.bin.pipeline_worker import process_queue
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.paths import loading_pipeline_queue_dir
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'


class PipelineWorkerTest(MockedDatarootTestCase):
    @patch('v03_pipeline.bin.pipeline_worker.safe_post_to_slack_success')
    @patch('v03_pipeline.bin.pipeline_worker.safe_post_to_slack_failure')
    @patch('v03_pipeline.bin.pipeline_worker.WriteSuccessFileTask')
    @patch('v03_pipeline.bin.pipeline_worker.logger')
    def test_main(
        self,
        mock_logger,
        mock_write_success_file_task,
        mock_slack_failure,
        mock_slack_success,
    ):
        run_id = '20250916-200704'
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
                f'request_{run_id}_1234.json',
            ),
            'w',
        ) as f:
            json.dump(raw_request, f)
        process_queue(local_scheduler=True)
        mock_slack_failure.assert_not_called()
        mock_slack_success.assert_called_once_with(123)
