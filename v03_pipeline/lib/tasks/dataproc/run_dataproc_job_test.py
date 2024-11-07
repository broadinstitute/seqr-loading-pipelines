import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import google.api_core.exceptions
import luigi

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.dataproc.run_dataproc_job import (
    RunDataprocJobTask,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask


@patch(
    'v03_pipeline.lib.tasks.dataproc.run_dataproc_job.CreateDataprocClusterTask',
)
@patch(
    'v03_pipeline.lib.tasks.dataproc.run_dataproc_job.dataproc.JobControllerClient',
)
class CreateDataprocClusterTaskTest(unittest.TestCase):
    def test_job_already_exists_failed(
        self,
        mock_job_controller_client: Mock,
        mock_create_dataproc_cluster: Mock,
    ) -> None:
        mock_create_dataproc_cluster.return_value = MockCompleteTask()
        mock_client = mock_job_controller_client.return_value
        mock_client.get_job.return_value = SimpleNamespace(
            status=SimpleNamespace(state='ERROR'),
        )
        worker = luigi.worker.Worker()
        task = RunDataprocJobTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='1',
            job_id='run_pipeline',
        )
        worker.add(task)
        worker.run()
        self.assertFalse(task.complete())

    def test_job_already_exists_success(
        self,
        mock_job_controller_client: Mock,
        mock_create_dataproc_cluster: Mock,
    ) -> None:
        mock_create_dataproc_cluster.return_value = MockCompleteTask()
        mock_client = mock_job_controller_client.return_value
        mock_client.get_job.return_value = SimpleNamespace(
            status=SimpleNamespace(state='DONE'),
        )
        worker = luigi.worker.Worker()
        task = RunDataprocJobTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='2',
            job_id='run_pipeline',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

    @patch('v03_pipeline.lib.tasks.dataproc.run_dataproc_job.logger')
    def test_job_failed(
        self,
        mock_job_controller_client: Mock,
        mock_create_dataproc_cluster: Mock,
        mock_logger: Mock,
    ) -> None:
        mock_create_dataproc_cluster.return_value = MockCompleteTask()
        mock_client = mock_job_controller_client.return_value
        mock_client.get_job.side_effect = google.api_core.exceptions.NotFound(
            'job not found',
        )
        operation = mock_client.submit_job_as_operation.return_value
        operation.done.side_effect = [False, True]
        operation.result.side_effect = Exception(
            'FailedPrecondition: 400 Job failed with message',
        )

        worker = luigi.worker.Worker()
        task = RunDataprocJobTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='2',
            job_id='run_pipeline',
        )
        worker.add(task)
        worker.run()
        self.assertFalse(task.complete())
        mock_logger.info.assert_has_calls(
            [call('Waiting for job completion run_pipeline-2')],
        )
