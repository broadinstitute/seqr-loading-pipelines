import datetime
import shutil
import tempfile
import unittest
from unittest import mock

import hail as hl
import luigi.worker

from seqr_gcnv_loading import SeqrGCNVVariantMTTask, SeqrGCNVGenotypesMTTask, SeqrGCNVMTToESTask
from lib.model.gcnv_mt_schema import parse_genes, hl_agg_collect_set_union

MERGED_CALLSET = 'tests/data/gcnv_merged_callset.tsv'
NEW_JOINED_CALLED_CALLSET = 'tests/data/gcnv_new_joint_called_callset.tsv'

SAMPLE_ID_REGEX = '(?P<sample_id>.+)_v\d+_Exome_(C|RP-)\d+$'

class SeqrGCNVGeneParsingTest(unittest.TestCase):
    def test_parse_genes(self):
        t1 = hl.Table.parallelize(
            [
                {"genes": "AC118553.2,SLC35A3"}, 
                {"genes": "AC118553.1,None"},
                {"genes": "None"},
                {"genes": "SLC35A3.43"},
                {"genes": ""},
                {"genes": "SLC35A4.43"},
            ], 
            hl.tstruct(genes=hl.dtype('str')), 
            key="genes"
        )
        t1 = t1.annotate(gene_set = parse_genes(t1.genes))
        self.assertCountEqual(
            t1.collect(),
            [
                hl.Struct(genes="AC118553.2,SLC35A3", gene_set=set(["AC118553", "SLC35A3"])),
                hl.Struct(genes="AC118553.1,None", gene_set=set(["AC118553"])),
                hl.Struct(genes="None", gene_set=set()),
                hl.Struct(genes="SLC35A3.43", gene_set=set(["SLC35A3"])),
                hl.Struct(genes="", gene_set=set()),
                hl.Struct(genes="SLC35A4.43", gene_set=set(["SLC35A4"])),
            ],
        )

    def test_aggregate_parsed_genes(self):
        t1 = hl.Table.parallelize(
            [
                {"genes": "AC118553.2,SLC35A3"}, 
                {"genes": "AC118553.1,None"}, 
                {"genes": "None"},
                {"genes": "SLC35A3.43"}, 
                {"genes": ""}, 
                {"genes": "SLC35A4.43"},
            ], 
            hl.tstruct(genes=hl.dtype('str')), 
            key="genes"
        )
        aggregated_gene_set = t1.aggregate(
            hl_agg_collect_set_union(parse_genes(t1.genes))
        )
        self.assertEqual(
            aggregated_gene_set,
        set(["SLC35A4", "SLC35A3", "AC118553"])
        )

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
            dest_path=self._genotypes_mt_file,
            is_new_joint_call=True,
        )
        worker.add(genotype_task)
        worker.run()

        variant_mt = hl.read_matrix_table(self._variant_mt_file)
        self.assertEqual(variant_mt.count(), (2, 3))

    @mock.patch('lib.model.gcnv_mt_schema.datetime', wraps=datetime)
    def test_run_merged_tsv_task(self, mock_datetime):
        mock_datetime.date.today.return_value = datetime.date(2022, 12, 2)
        worker = luigi.worker.Worker()
        SeqrGCNVVariantMTTask.source_paths = MERGED_CALLSET
        SeqrGCNVVariantMTTask.dest_path = self._variant_mt_file
        genotype_task = SeqrGCNVGenotypesMTTask(
            genome_version="38",
            source_paths="i am completely ignored",
            dest_path=self._genotypes_mt_file,
            is_new_joint_call=False,
        )
        worker.add(genotype_task)
        worker.run()

        variant_mt = hl.read_matrix_table(self._variant_mt_file)
        self.assertEqual(variant_mt.count(), (2, 3))