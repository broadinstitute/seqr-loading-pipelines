import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import google.api_core.exceptions
import luigi

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.dataproc.write_success_file_on_dataproc import (
    WriteSuccessFileOnDataprocTask,
)
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask


@patch(
    'v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc.CreateDataprocClusterTask',
)
@patch(
    'v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc.dataproc.JobControllerClient',
)
class WriteSuccessFileOnDataprocTaskTest(unittest.TestCase):
    @patch('v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc.logger')
    def test_job_already_exists_failed(
        self,
        mock_logger: Mock,
        mock_job_controller_client: Mock,
        mock_create_dataproc_cluster: Mock,
    ) -> None:
        mock_create_dataproc_cluster.return_value = MockCompleteTask()
        mock_client = mock_job_controller_client.return_value
        mock_client.get_job.return_value = SimpleNamespace(
            status=SimpleNamespace(
                state='ERROR',
                details='Google Cloud Dataproc Agent reports job failure. If logs are available, they can be found at...',
            ),
        )
        mock_client.submit_job_as_operation.side_effect = (
            google.api_core.exceptions.AlreadyExists('job exists')
        )
        worker = luigi.worker.Worker()
        task = WriteSuccessFileOnDataprocTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='test_callset',
            project_guids=['R0113_test_project'],
            project_remap_paths=['test_remap'],
            project_pedigree_paths=['test_pedigree'],
            run_id='manual__2024-04-03',
        )
        worker.add(task)
        worker.run()
        self.assertFalse(task.complete())
        mock_logger.error.assert_has_calls(
            [call('Job WriteSuccessFileTask-manual__2024-04-03 entered ERROR state')],
        )

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
        task = WriteSuccessFileOnDataprocTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='test_callset',
            project_guids=['R0113_test_project'],
            project_remap_paths=['test_remap'],
            project_pedigree_paths=['test_pedigree'],
            run_id='manual__2024-04-04',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

    @patch('v03_pipeline.lib.tasks.dataproc.base_run_job_on_dataproc.logger')
    def test_job_failed(
        self,
        mock_logger: Mock,
        mock_job_controller_client: Mock,
        mock_create_dataproc_cluster: Mock,
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
        task = WriteSuccessFileOnDataprocTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='test_callset',
            project_guids=['R0113_test_project'],
            project_remap_paths=['test_remap'],
            project_pedigree_paths=['test_pedigree'],
            run_id='manual__2024-04-05',
        )
        worker.add(task)
        worker.run()
        self.assertFalse(task.complete())
        mock_logger.info.assert_has_calls(
            [
                call(
                    'Waiting for job completion WriteSuccessFileTask-manual__2024-04-05',
                ),
            ],
        )

    def test_job_success(
        self,
        mock_job_controller_client: Mock,
        mock_create_dataproc_cluster: Mock,
    ) -> None:
        mock_create_dataproc_cluster.return_value = MockCompleteTask()
        mock_client = mock_job_controller_client.return_value
        mock_client.get_job.side_effect = google.api_core.exceptions.NotFound(
            'job not found',
        )
        operation = mock_client.submit_job_as_operation.return_value
        operation.done.side_effect = [False, True]
        worker = luigi.worker.Worker()
        task = WriteSuccessFileOnDataprocTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='test_callset',
            project_guids=['R0113_test_project'],
            project_remap_paths=['test_remap'],
            project_pedigree_paths=['test_pedigree'],
            run_id='manual__2024-04-06',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())
