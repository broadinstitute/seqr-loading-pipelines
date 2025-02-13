import json
from decimal import Decimal
from unittest.mock import Mock, patch

import google.cloud.bigquery
import hailtop.fs as hfs
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType
from v03_pipeline.lib.tasks.write_sample_qc_json import WriteSampleQCJsonTask
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_VCF = 'v03_pipeline/var/test/callsets/1kg_30variants.vcf'
TEST_RUN_ID = 'manual__2024-04-03'


class WriteSampleQCJsonTaskTest(MockedDatarootTestCase):
    @patch('v03_pipeline.lib.tasks.write_tdr_metrics_files.gen_bq_table_names')
    @patch('v03_pipeline.lib.tasks.write_tdr_metrics_file.bq_metrics_query')
    def test_call_sample_qc(
        self,
        mock_bq_metrics_query: Mock,
        mock_gen_bq_table_names: Mock,
    ) -> None:
        mock_gen_bq_table_names.return_value = ['datarepo-7242affb.datarepo_RP_0113']
        mock_bq_metrics_query.side_effect = [
            iter(
                [
                    google.cloud.bigquery.table.Row(
                        (
                            'HG00731',
                            'Unknown',
                            Decimal('5.1'),
                            Decimal('93.69'),
                            Decimal('29.31'),
                        ),
                        {
                            'collaborator_sample_id': 0,
                            'predicted_sex': 1,
                            'contamination_rate': 2,
                            'percent_bases_at_20x': 3,
                            'mean_coverage': 4,
                        },
                    ),
                    google.cloud.bigquery.table.Row(
                        (
                            'HG00732',
                            'Female',
                            Decimal('5'),
                            Decimal('90'),
                            Decimal('28'),
                        ),
                        {
                            'collaborator_sample_id': 0,
                            'predicted_sex': 1,
                            'contamination_rate': 2,
                            'percent_bases_at_20x': 3,
                            'mean_coverage': 4,
                        },
                    ),
                    google.cloud.bigquery.table.Row(
                        (
                            'HG00733',
                            'Male',
                            Decimal('6'),
                            Decimal('85'),
                            Decimal('36.4'),
                        ),
                        {
                            'collaborator_sample_id': 0,
                            'predicted_sex': 1,
                            'contamination_rate': 2,
                            'percent_bases_at_20x': 3,
                            'mean_coverage': 4,
                        },
                    ),
                    google.cloud.bigquery.table.Row(
                        ('NA19675', 'Male', Decimal('0'), Decimal('80'), Decimal('30')),
                        {
                            'collaborator_sample_id': 0,
                            'predicted_sex': 1,
                            'contamination_rate': 2,
                            'percent_bases_at_20x': 3,
                            'mean_coverage': 4,
                        },
                    ),
                ],
            ),
        ]
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
            self.assertDictEqual(
                json.load(f),
                {
                    'HG00731': {
                        'sample_type': 'WGS',
                        'filter_flags': ['contamination', 'coverage'],
                    },
                    'HG00732': {'sample_type': 'WGS', 'filter_flags': ['coverage']},
                    'HG00733': {
                        'sample_type': 'WGS',
                        'filter_flags': ['contamination'],
                    },
                    'NA19675': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA19678': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA19679': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20870': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20872': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20874': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20875': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20876': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20877': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20878': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20881': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20885': {'sample_type': 'WGS', 'filter_flags': []},
                    'NA20888': {'sample_type': 'WGS', 'filter_flags': []},
                },
            )
