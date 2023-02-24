import datetime
import shutil
import tempfile
import unittest
from unittest import mock

import hail as hl
import luigi.worker

from seqr_gcnv_loading import SeqrGCNVVariantMTTask, SeqrGCNVGenotypesMTTask, SeqrGCNVMTToESTask

MERGED_CALLSET = 'tests/data/gcnv_merged_callset.tsv'
NEW_JOINED_CALLED_CALLSET = 'tests/data/gcnv_new_joint_called_callset.tsv'

SAMPLE_ID_REGEX = '(?P<sample_id>.+)_v\d+_Exome_(C|RP-)\d+$'

class SeqrGCNVLoadingTest(unittest.TestCase):
    def setUp(self):
        self._temp_dir = tempfile.TemporaryDirectory()
        self._variant_mt_file = tempfile.mkstemp(dir=self._temp_dir.name, suffix='.mt')[1]
        self._genotypes_mt_file = tempfile.mkstemp(dir=self._temp_dir.name, suffix='.mt')[1]

    def tearDown(self):
        shutil.rmtree(self._temp_dir.name)

    @mock.patch('lib.model.gcnv_mt_schema.datetime', wraps=datetime)
    def test_run_new_joint_tsv_task(self, mock_datetime):
        mock_datetime.date.today.return_value = datetime.date(2022, 12, 2)
        worker = luigi.worker.Worker()
        SeqrGCNVVariantMTTask.source_paths = NEW_JOINED_CALLED_CALLSET
        SeqrGCNVVariantMTTask.dest_path = self._variant_mt_file
        genotype_task = SeqrGCNVGenotypesMTTask(
            genome_version="38",
            source_paths="i am completely ignored",
            dest_path=self._genotypes_mt_file
        )
        worker.add(genotype_task)
        worker.run()

        variant_mt = hl.read_matrix_table(self._variant_mt_file)
        self.assertEqual(variant_mt.count(), (11, 5))

    @mock.patch('lib.model.gcnv_mt_schema.datetime', wraps=datetime)
    def test_run_merged_tsv_task(self, mock_datetime):
        mock_datetime.date.today.return_value = datetime.date(2022, 12, 2)
        worker = luigi.worker.Worker()
        SeqrGCNVVariantMTTask.source_paths = MERGED_CALLSET
        SeqrGCNVVariantMTTask.dest_path = self._variant_mt_file
        genotype_task = SeqrGCNVGenotypesMTTask(
            genome_version="38",
            source_paths="i am completely ignored",
            dest_path=self._genotypes_mt_file
        )
        worker.add(genotype_task)
        worker.run()