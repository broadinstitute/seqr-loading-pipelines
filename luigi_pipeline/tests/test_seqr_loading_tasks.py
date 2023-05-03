import unittest
from unittest.mock import patch

import hail as hl

from luigi_pipeline.seqr_loading import SeqrValidationError, SeqrVCFToMTTask

TEST_DATA_MT_1KG = 'tests/data/1kg_30variants.vcf.bgz'


@patch('seqr_loading.SeqrVCFToMTTask.contig_check', return_value={})
class TestSeqrLoadingTasks(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_mt = hl.import_vcf(TEST_DATA_MT_1KG)

    def _sample_type_stats_return_value(
        self,
        nc_match_count,
        nc_total_count,
        nc_match,
        c_match_count,
        c_total_count,
        c_match,
    ):
        return {
            'noncoding': {
                'matched_count': nc_match_count,
                'total_count': nc_total_count,
                'match': nc_match,
            },
            'coding': {
                'matched_count': c_match_count,
                'total_count': c_total_count,
                'match': c_match,
            },
        }

    @patch('lib.hail_tasks.HailMatrixTableTask.sample_type_stats')
    def test_seqr_loading_validate_non_mt(
        self, mock_sample_type_stats, mock_contig_check,
    ):
        mock_sample_type_stats.return_value = None
        self.assertRaises(
            SeqrValidationError, SeqrVCFToMTTask.validate_mt, None, '37', None,
        )
        self.assertRaises(
            SeqrValidationError, SeqrVCFToMTTask.validate_mt, 12345, '37', None,
        )
        self.assertRaises(
            SeqrValidationError, SeqrVCFToMTTask.validate_mt, {}, '37', None,
        )

    @patch('lib.hail_tasks.HailMatrixTableTask.sample_type_stats')
    def test_seqr_loading_validate_match_none(
        self, mock_sample_type_stats, mock_contig_check,
    ):
        # Matched none should fail.
        mock_sample_type_stats.return_value = self._sample_type_stats_return_value(
            0, 0, False, 0, 0, False,
        )
        self.assertRaises(
            SeqrValidationError, SeqrVCFToMTTask.validate_mt, self.test_mt, '37', None,
        )

    @patch('lib.hail_tasks.HailMatrixTableTask.sample_type_stats')
    def test_seqr_loading_validate_match_both(
        self, mock_sample_type_stats, mock_contig_check,
    ):
        # Proper WGS, should pass.
        mock_sample_type_stats.return_value = self._sample_type_stats_return_value(
            0, 0, True, 0, 0, True,
        )
        self.assertTrue(SeqrVCFToMTTask.validate_mt(self.test_mt, '37', 'WGS'))

    @patch('lib.hail_tasks.HailMatrixTableTask.sample_type_stats')
    def test_seqr_loading_validate_match_coding_only(
        self, mock_sample_type_stats, mock_contig_check,
    ):
        # Proper WES, should pass.
        mock_sample_type_stats.return_value = self._sample_type_stats_return_value(
            0, 0, False, 0, 0, True,
        )
        self.assertTrue(SeqrVCFToMTTask.validate_mt(self.test_mt, '37', 'WES'))

    @patch('lib.hail_tasks.HailMatrixTableTask.sample_type_stats')
    def test_seqr_loading_validate_match_noncoding_only(
        self, mock_sample_type_stats, mock_contig_check,
    ):
        # We never use non coding only.
        mock_sample_type_stats.return_value = self._sample_type_stats_return_value(
            0, 0, True, 0, 0, False,
        )
        self.assertRaises(
            SeqrValidationError, SeqrVCFToMTTask.validate_mt, self.test_mt, '37', None,
        )

    @patch('lib.hail_tasks.HailMatrixTableTask.sample_type_stats')
    def test_seqr_loading_validate_wes_mismatch(
        self, mock_sample_type_stats, mock_contig_check,
    ):
        # Supposed to be WES but we report as WGS.
        mock_sample_type_stats.return_value = self._sample_type_stats_return_value(
            0, 0, False, 0, 0, True,
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'specified as WGS but appears to be WES',
            SeqrVCFToMTTask.validate_mt,
            self.test_mt,
            '37',
            'WGS',
        )

    @patch('lib.hail_tasks.HailMatrixTableTask.sample_type_stats')
    def test_seqr_loading_validate_wgs_mismatch(
        self, mock_sample_type_stats, mock_contig_check,
    ):
        # Supposed to be WGS but we report as WES.
        mock_sample_type_stats.return_value = self._sample_type_stats_return_value(
            0, 0, True, 0, 0, True,
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'specified as WES but appears to be WGS',
            SeqrVCFToMTTask.validate_mt,
            self.test_mt,
            '37',
            'WGS',
        )
