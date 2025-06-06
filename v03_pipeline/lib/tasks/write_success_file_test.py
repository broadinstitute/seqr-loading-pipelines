from unittest import mock

import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_success_file import WriteSuccessFileTask
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class WriteSuccessFileTaskTest(MockedDatarootTestCase):
    @mock.patch(
        'v03_pipeline.lib.tasks.write_success_file.RunPipelineTask',
    )
    def test_write_success_file_task(
        self,
        mock_run_pipeline_task: mock.Mock,
    ) -> None:
        mock_run_pipeline_task.return_value = MockCompleteTask()
        worker = luigi.worker.Worker()
        write_success_file = WriteSuccessFileTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            sample_type=SampleType.WGS,
            callset_path='test_callset',
            project_guids=['R0113_test_project'],
            project_pedigree_paths=['test_pedigree'],
            run_id='manual__2024-04-03',
        )
        worker.add(write_success_file)
        worker.run()
        self.assertTrue(write_success_file.complete())
        with open(write_success_file.output().path) as f:
            self.assertEqual(f.read(), '')
