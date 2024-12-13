import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import google.api_core.exceptions
import luigi

from v03_pipeline.lib.misc.gcp import SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster import (
    CreateDataprocClusterTask,
)


@patch(
    'v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster.get_service_account_credentials',
    return_value=SimpleNamespace(
        service_account_email='test@serviceaccount.com',
        scopes=SOCIAL_AUTH_GOOGLE_OAUTH2_SCOPE,
    ),
)
@patch(
    'v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster.dataproc.ClusterControllerClient',
)
class CreateDataprocClusterTaskTest(unittest.TestCase):
    def test_dataset_type_unsupported(
        self,
        mock_cluster_controller: Mock,
        _: Mock,
    ) -> None:
        worker = luigi.worker.Worker()
        task = CreateDataprocClusterTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.MITO,
            run_id='1',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

    def test_spinup_cluster_already_exists_failed(
        self,
        mock_cluster_controller: Mock,
        _: Mock,
    ) -> None:
        mock_client = mock_cluster_controller.return_value
        mock_client.get_cluster.return_value = SimpleNamespace(
            status=SimpleNamespace(state='FAILED'),
        )
        mock_client.create_cluster.side_effect = (
            google.api_core.exceptions.AlreadyExists('cluster exists')
        )
        worker = luigi.worker.Worker()
        task = CreateDataprocClusterTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='2',
        )
        worker.add(task)
        worker.run()
        self.assertFalse(task.complete())

    def test_spinup_cluster_already_exists_success(
        self,
        mock_cluster_controller: Mock,
        _: Mock,
    ) -> None:
        mock_client = mock_cluster_controller.return_value
        mock_client.get_cluster.return_value = SimpleNamespace(
            status=SimpleNamespace(state='RUNNING'),
        )
        mock_client.create_cluster.side_effect = (
            google.api_core.exceptions.AlreadyExists('cluster exists')
        )
        worker = luigi.worker.Worker()
        task = CreateDataprocClusterTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='3',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.complete())

    @patch('v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster.logger')
    def test_spinup_cluster_doesnt_exist_failed(
        self,
        mock_logger: Mock,
        mock_cluster_controller: Mock,
        _: Mock,
    ) -> None:
        mock_client = mock_cluster_controller.return_value
        mock_client.get_cluster.side_effect = google.api_core.exceptions.NotFound(
            'cluster not found',
        )
        operation = mock_client.create_cluster.return_value
        operation.done.side_effect = [False, True]
        operation.result.side_effect = Exception('SpinupFailed')

        worker = luigi.worker.Worker()
        task = CreateDataprocClusterTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='4',
        )
        worker.add(task)
        worker.run()
        self.assertFalse(task.complete())
        mock_logger.info.assert_has_calls([call('Waiting for cluster spinup')])

    @patch('v03_pipeline.lib.tasks.dataproc.create_dataproc_cluster.logger')
    def test_spinup_cluster_doesnt_exist_success(
        self,
        mock_logger: Mock,
        mock_cluster_controller: Mock,
        _: Mock,
    ) -> None:
        mock_client = mock_cluster_controller.return_value
        mock_client.get_cluster.side_effect = google.api_core.exceptions.NotFound(
            'cluster not found',
        )
        operation = mock_client.create_cluster.return_value
        operation.done.side_effect = [False, True]
        operation.result.return_value = SimpleNamespace(
            cluster_name='dataproc-cluster-1',
            cluster_uuid='12345',
        )
        worker = luigi.worker.Worker()
        task = CreateDataprocClusterTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='5',
        )
        worker.add(task)
        worker.run()
        mock_logger.info.assert_has_calls(
            [
                call('Waiting for cluster spinup'),
                call('Created cluster dataproc-cluster-1 with cluster uuid: 12345'),
            ],
        )
