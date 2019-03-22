import unittest
from unittest.mock import patch

import hail as hl

from seqr_loading import SeqrVCFToMTTask, SeqrValidationError


class TestSeqrLoadingTasks(unittest.TestCase):

    def _impute_return_value(self, nc_match_count, nc_total_count, nc_match, c_match_count, c_total_count, c_match):
        return {
            'noncoding': {'matched_count': nc_match_count, 'total_count': nc_total_count, 'match': nc_match},
            'coding': {'matched_count': c_match_count, 'total_count': c_total_count, 'match': c_match}
        }

    @patch('lib.hail_tasks.HailMatrixTableTask.hl_mt_impute_sample_type')
    def test_seqr_loading_validate_match_none(self, mock_impute):
        # Matched none should fail.
        mock_impute.return_value = self._impute_return_value(0, 0, False, 0, 0, False)
        self.assertRaises(SeqrValidationError, SeqrVCFToMTTask.validate_mt, None, '37', None)

    @patch('lib.hail_tasks.HailMatrixTableTask.hl_mt_impute_sample_type')
    def test_seqr_loading_validate_match_both(self, mock_impute):
        # Proper WGS, should pass.
        mock_impute.return_value = self._impute_return_value(0, 0, True, 0, 0, True)
        self.assertTrue(SeqrVCFToMTTask.validate_mt(None, '37', 'WGS'))

    @patch('lib.hail_tasks.HailMatrixTableTask.hl_mt_impute_sample_type')
    def test_seqr_loading_validate_match_coding_only(self, mock_impute):
        # Proper WES, should pass.
        mock_impute.return_value = self._impute_return_value(0, 0, False, 0, 0, True)
        self.assertTrue(SeqrVCFToMTTask.validate_mt(None, '37', 'WES'))

    @patch('lib.hail_tasks.HailMatrixTableTask.hl_mt_impute_sample_type')
    def test_seqr_loading_validate_match_noncoding_only(self, mock_impute):
        # We never use non coding only.
        mock_impute.return_value = self._impute_return_value(0, 0, True, 0, 0, False)
        self.assertRaises(SeqrValidationError, SeqrVCFToMTTask.validate_mt, None, '37', None)

    @patch('lib.hail_tasks.HailMatrixTableTask.hl_mt_impute_sample_type')
    def test_seqr_loading_validate_wes_mismatch(self, mock_impute):
        # Supposed to be WES but we report as WGS.
        mock_impute.return_value = self._impute_return_value(0, 0, False, 0, 0, True)
        self.assertRaises(SeqrValidationError, SeqrVCFToMTTask.validate_mt, None, '37', 'WGS')

    @patch('lib.hail_tasks.HailMatrixTableTask.hl_mt_impute_sample_type')
    def test_seqr_loading_validate_wgs_mismatch(self, mock_impute):
        # Supposed to be WGS but we report as WES.
        mock_impute.return_value = self._impute_return_value(0, 0, True, 0, 0, True)
        self.assertRaises(SeqrValidationError, SeqrVCFToMTTask.validate_mt, None, '37', 'WES')
