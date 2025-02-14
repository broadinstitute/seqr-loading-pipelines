import json
from decimal import Decimal
from unittest.mock import Mock, patch

import google.cloud.bigquery
import hailtop.fs as hfs
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_sample_qc_json import WriteSampleQCJsonTask
from v03_pipeline.lib.test.mock_complete_task import MockCompleteTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_TDR_METRICS_FILE = 'v03_pipeline/var/test/tdr_metrics.tsv'
TEST_RUN_ID = 'manual__2024-04-03'


class WriteSampleQCJsonTaskTest(MockedDatarootTestCase):
    @patch('v03_pipeline.lib.tasks.write_sample_qc_json.WriteTDRMetricsFilesTask')
    @patch('v03_pipeline.lib.tasks.write_sample_qc_json.hfs.ls')
    def test_call_sample_qc(
        self,
        mock_ls_tdr_dir: Mock,
        mock_tdr_task: Mock,
    ) -> None:
        mock_tdr_task.return_value = MockCompleteTask()
        mock_tdr_table = Mock()
        mock_tdr_table.path = TEST_TDR_METRICS_FILE
        mock_ls_tdr_dir.return_value = [mock_tdr_table]

        worker = luigi.worker.Worker()
        task = WriteSampleQCJsonTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            run_id=TEST_RUN_ID,
            sample_type=SampleType.WGS,
            callset_path=TEST_VCF,
            project_guids=['R0113_test_project'],
            skip_validation=True,
        )
        worker.add(task)
        worker.run()
        self.assertTrue(task)
        self.assertTrue(hfs.exists(task.output().path))

        with task.output().open('r') as f:
            res = json.load(f)

        self.assertCountEqual(
            res['HG00731'],
            {
                'sample_type': 'WGS',
                'filter_flags': ['contamination', 'coverage'],
            },
        )
        self.assertCountEqual(
            res['HG00732'],
            {'sample_type': 'WGS', 'filter_flags': ['coverage']},
        )
        self.assertCountEqual(
            res['HG00733'],
            {
                'sample_type': 'WGS',
                'filter_flags': ['contamination'],
            },
        )
        self.assertCountEqual(
            res['HG00732'],
            {'sample_type': 'WGS', 'filter_flags': []},
        )
