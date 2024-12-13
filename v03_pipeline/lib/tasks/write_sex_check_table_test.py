from unittest.mock import Mock, patch

import google.cloud.bigquery
import hail as hl
import luigi.worker

from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.paths import sex_check_table_path, tdr_metrics_path
from v03_pipeline.lib.tasks.write_sex_check_table import (
    WriteSexCheckTableTask,
)
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase


class WriteSexCheckTableTaskTest(MockedDatarootTestCase):
    @patch('v03_pipeline.lib.tasks.write_tdr_metrics_files.gen_bq_table_names')
    @patch('v03_pipeline.lib.tasks.write_tdr_metrics_file.bq_metrics_query')
    def test_snv_sex_check_table_task(
        self,
        mock_bq_metrics_query: Mock,
        mock_gen_bq_table_names: Mock,
    ) -> None:
        mock_gen_bq_table_names.return_value = [
            'datarepo-7242affb.datarepo_RP_3053',
            'datarepo-5a72e31b.datarepo_RP_3056',
        ]
        mock_bq_metrics_query.side_effect = [
            iter(
                [
                    google.cloud.bigquery.table.Row(
                        ('SM-NJ8MF', 'Unknown'),
                        {'collaborator_sample_id': 0, 'predicted_sex': 1},
                    ),
                    google.cloud.bigquery.table.Row(
                        ('SM-MWOGC', 'Female'),
                        {'collaborator_sample_id': 0, 'predicted_sex': 1},
                    ),
                    google.cloud.bigquery.table.Row(
                        ('SM-MWKWL', 'Male'),
                        {'collaborator_sample_id': 0, 'predicted_sex': 1},
                    ),
                ],
            ),
            iter(
                [
                    google.cloud.bigquery.table.Row(
                        ('SM-NGE65', 'Male'),
                        {'collaborator_sample_id': 0, 'predicted_sex': 1},
                    ),
                    google.cloud.bigquery.table.Row(
                        ('SM-NGE5G', 'Male'),
                        {'collaborator_sample_id': 0, 'predicted_sex': 1},
                    ),
                    google.cloud.bigquery.table.Row(
                        ('SM-NC6LM', 'Male'),
                        {'collaborator_sample_id': 0, 'predicted_sex': 1},
                    ),
                ],
            ),
        ]
        worker = luigi.worker.Worker()
        write_sex_check_table = WriteSexCheckTableTask(
            reference_genome=ReferenceGenome.GRCh38,
            dataset_type=DatasetType.SNV_INDEL,
            callset_path='na',
        )
        worker.add(write_sex_check_table)
        worker.run()
        self.assertTrue(write_sex_check_table.complete())
        sex_check_ht = hl.read_table(
            sex_check_table_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'na',
            ),
        )
        self.assertEqual(
            sex_check_ht.collect(),
            [
                hl.Struct(s='SM-MWKWL', predicted_sex='M'),
                hl.Struct(s='SM-MWOGC', predicted_sex='F'),
                hl.Struct(s='SM-NC6LM', predicted_sex='M'),
                hl.Struct(s='SM-NGE5G', predicted_sex='M'),
                hl.Struct(s='SM-NGE65', predicted_sex='M'),
                hl.Struct(s='SM-NJ8MF', predicted_sex='U'),
            ],
        )
        # Check underlying tdr metrics file.
        with open(
            tdr_metrics_path(
                ReferenceGenome.GRCh38,
                DatasetType.SNV_INDEL,
                'datarepo-5a72e31b.datarepo_RP_3056',
            ),
        ) as f:
            self.assertTrue('collaborator_sample_id' in f.read())
