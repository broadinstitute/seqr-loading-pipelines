import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

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
