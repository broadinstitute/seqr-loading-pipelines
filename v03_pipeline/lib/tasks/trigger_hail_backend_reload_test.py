from unittest.mock import Mock, patch

import luigi.worker
import requests

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.tasks.trigger_hail_backend_reload import TriggerHailBackendReload
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class TriggerHailBackendReloadTestCase(MockedDatarootTestCase):
    @patch.object(requests, 'post')
    def test_success(self, mock_post: Mock):
        mock_resp = requests.models.Response()
        mock_resp.status_code = 200
        mock_post.return_value = mock_resp

        worker = luigi.worker.Worker()
        task = TriggerHailBackendReload(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='manual__2024-09-19',
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task.output().exists())
        self.assertTrue(task.complete())

    @patch.object(requests, 'post')
    def test_failure(self, mock_post: Mock):
        mock_resp = requests.models.Response()
        mock_resp.status_code = 500
        mock_post.return_value = mock_resp

        worker = luigi.worker.Worker()
        task = TriggerHailBackendReload(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id='manual__2024-09-19',
        )
        worker.add(task)
        self.assertFalse(task.output().exists())
        self.assertFalse(task.complete())
