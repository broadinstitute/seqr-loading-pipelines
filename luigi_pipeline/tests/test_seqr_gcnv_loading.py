import datetime
import shutil
import tempfile
import unittest
from unittest import mock

import hail as hl
import luigi.worker

from seqr_gcnv_loading import SeqrGCNVVariantMTTask, SeqrGCNVGenotypesMTTask, SeqrGCNVMTToESTask
from lib.model.gcnv_mt_schema import parse_genes, hl_agg_collect_set_union

NEW_JOINT_CALLED_CALLSET = 'tests/data/gcnv_new_joint_called_callset.tsv'
NEW_JOINT_CALLED_EXPECTED_VARIANT_DATA = [
    hl.Struct(
        contig='1',
        sc=1,
        sf=4.401408e-05,
        svType='DEL',
        StrVCTVRE_score=0.583,
        variantId='suffix_16453_DEL_12022022',
        start=100006937,
        end=100007881,
        num_exon=2,
        geneIds=['ENSG00000117620', 'ENSG00000283761'],
        pos=100006937,
        sn=22720,
        sortedTranscriptConsequences=[
            {'gene_id': 'ENSG00000117620', 'major_consequence': 'LOF'},
            {'gene_id': 'ENSG00000283761', 'major_consequence': 'LOF'}
        ],
        transcriptConsequenceTerms=['LOF', 'gCNV_DEL'],
        xpos=1100006937,
        xstart=1100006937,
        xstop=1100007881
    ),
    hl.Struct(
        contig='1',
        sc=2,
        sf=8.802817e-05,
        svType='DEL',
        StrVCTVRE_score=0.507,
        variantId='suffix_16456_DEL_12022022',
        start=100017585,
        end=100023213,
        num_exon=3,
        geneIds=['ENSG00000117620', 'ENSG00000283761'],
        pos=100017585,
        sn=22719,
        sortedTranscriptConsequences=[
            {'gene_id': 'ENSG00000117620', 'major_consequence': 'LOF'},
            {'gene_id': 'ENSG00000283761', 'major_consequence': 'LOF'}
        ],
        transcriptConsequenceTerms=['LOF', 'gCNV_DEL'],
        xpos=1100017585,
        xstart=1100017585,
        xstop=1100023213,
    ),
    hl.Struct(
        contig='1',
        sc=1,
        sf=4.401408e-05,
        svType='DEL',
        StrVCTVRE_score=0.502,
        variantId='suffix_16457_DEL_12022022',
        start=100022289,
        end=100023213, geneIds=['ENSG00000117620'], num_exon=1,
        pos=100022289,
        sn=22720, 
        sortedTranscriptConsequences=[
            {'gene_id': 'ENSG00000117620', 'major_consequence': 'LOF'}
        ],
        transcriptConsequenceTerms=['LOF', 'gCNV_DEL'], 
        xpos=1100022289,
        xstart=1100022289,
        xstop=1100023213
    ),
]
NEW_JOINT_CALLED_EXPECTED_GENOTYPES_DATA = [
    hl.Struct(
        genotypes=[
            hl.Struct(**{
                'cn': 1,
                'defragged': False,
                'new_call': False,
                'prev_call': True,
                'prev_overlap': True,
                'qs': 4,
                'sample_id': 'PIE_OGI2271_003780_D1'
            })
        ],
        samples=['PIE_OGI2271_003780_D1'], 
        **{
            'samples_cn.1': ['PIE_OGI2271_003780_D1'],
            'samples_qs.0_to_10': ['PIE_OGI2271_003780_D1'],
        }
    ),
    hl.Struct(
        genotypes=[
            hl.Struct(**{
                'qs': 30,
                'cn': 0, 
                'defragged': False, 
                'prev_overlap': True, 
                'new_call': False,
                'prev_call': True,
                'sample_id': 'MAN_0354_01_1'
            }),
            hl.Struct(**{
                'qs': 5,
                'cn': 1, 
                'defragged': False, 
                'prev_overlap': True, 
                'new_call': False,
                'prev_call': True,
                'sample_id':  'PIE_OGI313_000747_1'
            }),
        ],
        samples=['MAN_0354_01_1', 'PIE_OGI313_000747_1'], 
        **{
            'samples_cn.1': ['PIE_OGI313_000747_1'],
            'samples_qs.0_to_10': ['PIE_OGI313_000747_1'], 
            'samples_cn.0': ['MAN_0354_01_1'],
            'samples_qs.30_to_40': ['MAN_0354_01_1'],
        },
    ),
    hl.Struct(
        genotypes=[
            hl.Struct(**{
                'qs': 3, 
                'cn': 1, 
                'defragged': False, 
                'prev_overlap': False, 
                'new_call': True,
                'prev_call': False,
                'sample_id': 'GLE-4772-4-2-a',
            })
        ],
        samples=['GLE-4772-4-2-a'], 
        samples_new_call=['GLE-4772-4-2-a'],
        **{
            'samples_cn.1': ['GLE-4772-4-2-a'],
            'samples_qs.0_to_10': ['GLE-4772-4-2-a'],
        }
    ),
]
NEW_JOINT_CALLED_EXPECTED_VARIANT_AND_GENOTYPES_DATA = [
    hl.Struct(**x, **y) for (x, y) in zip(NEW_JOINT_CALLED_EXPECTED_VARIANT_DATA, NEW_JOINT_CALLED_EXPECTED_GENOTYPES_DATA)
]

MERGED_CALLSET = 'tests/data/gcnv_merged_callset.tsv'
MERGED_EXPECTED_VARIANT_DATA = [
    hl.Struct(
        contig='1',
        sc=1,
        sf=4.401408e-05,
        svType='DEL',
        StrVCTVRE_score=0.583,
        variantId='suffix_16453_DEL_12022022',
        start=100006937, 
        end=100007881, 
        num_exon=2,
        pos=100006937,
        sn=22720,
        geneIds=['ENSG00000117620', 'ENSG00000283761'],
        sortedTranscriptConsequences=[{'gene_id': 'ENSG00000117620', 'major_consequence': 'LOF'}, {'gene_id': 'ENSG00000283761', 'major_consequence': 'LOF'}],
        transcriptConsequenceTerms=['LOF', 'gCNV_DEL'],
        xpos=1100006937,
        xstart=1100006937,
        xstop=1100007881,
    ),
    hl.Struct(
        contig='1',
        sc=2,
        sf=8.802817e-05,
        svType='DEL',
        StrVCTVRE_score=0.507,
        variantId='suffix_16456_DEL_12022022',
        start=100017585, 
        end=100023213, 
        num_exon=3,
        pos=100017585,
        sn=22719,
        geneIds=['ENSG00000117620', 'ENSG00000283761'],
        sortedTranscriptConsequences=[{'gene_id': 'ENSG00000117620', 'major_consequence': 'LOF'}, {'gene_id': 'ENSG00000283761', 'major_consequence': 'LOF'}],
        transcriptConsequenceTerms=['LOF', 'gCNV_DEL'],
        xpos=1100017585,
        xstart=1100017585,
        xstop=1100023213,
    ),
]

MERGED_EXPECTED_GENOTYPES_DATA = [
    hl.Struct(
        genotypes=[
            hl.Struct(**{
                'cn': 1,
                'defragged': False,
                'new_call': False,
                'prev_call': True,
                'prev_overlap': False,
                'qs': 4,
                'sample_id': 'PIE_OGI2271_003780_D1'
            })
        ],
        samples=['PIE_OGI2271_003780_D1'], 
        **{
            'samples_cn.1': ['PIE_OGI2271_003780_D1'],
            'samples_qs.0_to_10': ['PIE_OGI2271_003780_D1'],
        }
    ),
    hl.Struct(
        genotypes=[
            hl.Struct(**{
                'qs': 30,
                'cn': 0, 
                'defragged': False, 
                'prev_overlap': False, 
                'new_call': False,
                'prev_call': True,
                'sample_id': 'MAN_0354_01_1'
            }),
            hl.Struct(**{
                'qs': 5,
                'cn': 1, 
                'defragged': False, 
                'prev_overlap': False, 
                'new_call': False,
                'prev_call': False,
                'sample_id':  'PIE_OGI313_000747_1'
            }),
        ],
        samples=['MAN_0354_01_1', 'PIE_OGI313_000747_1'], 
        **{
            'samples_cn.1': ['PIE_OGI313_000747_1'],
            'samples_qs.0_to_10': ['PIE_OGI313_000747_1'], 
            'samples_cn.0': ['MAN_0354_01_1'],
            'samples_qs.30_to_40': ['MAN_0354_01_1'],
        },
    ),
]
MERGED_EXPECTED_VARIANT_AND_GENOTYPES_DATA = [
    hl.Struct(**x, **y) for (x, y) in zip(MERGED_EXPECTED_VARIANT_DATA, MERGED_EXPECTED_GENOTYPES_DATA)
]


def prune_empties(data):
    for k, v in data.items():
        if v == None:
            data = data.drop(k)
    return data

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
                hl.Struct(genes="AC118553.2,SLC35A3", gene_set=["AC118553", "SLC35A3"]),
                hl.Struct(genes="AC118553.1,None", gene_set=["AC118553"]),
                hl.Struct(genes="None", gene_set=[]),
                hl.Struct(genes="SLC35A3.43", gene_set=["SLC35A3"]),
                hl.Struct(genes="", gene_set=[]),
                hl.Struct(genes="SLC35A4.43", gene_set=["SLC35A4"]),
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
        variant_task = SeqrGCNVVariantMTTask(
            source_paths=NEW_JOINT_CALLED_CALLSET,
            dest_path=self._variant_mt_file,
        )
        genotype_task = SeqrGCNVGenotypesMTTask(
            genome_version="38",
            source_paths="i am completely ignored",
            dest_path=self._genotypes_mt_file,
            is_new_joint_call=True,
        )
        SeqrGCNVGenotypesMTTask.requires = lambda self: [variant_task]
        worker.add(genotype_task)
        worker.run()

        variant_mt = hl.read_matrix_table(self._variant_mt_file)
        self.assertEqual(variant_mt.count(), (3, 4))

        key_dropped_variant_mt = variant_mt.rows().flatten().drop("variant_name", "svtype")
        data = key_dropped_variant_mt.collect()
        self.assertCountEqual(data, NEW_JOINT_CALLED_EXPECTED_VARIANT_DATA)

        # Genotypes (only) Assertions
        genotypes_mt = hl.read_matrix_table(self._genotypes_mt_file)
        self.assertEqual(genotypes_mt.count(), (3, 4))

        # Now mimic the join in BaseMTToESOptimizedTask
        genotypes_mt = genotypes_mt.drop(*[k for k in genotypes_mt.globals.keys()])
        row_ht = genotypes_mt.rows().join(variant_mt.rows()).flatten().drop("variant_name", "svtype")
        data = row_ht.collect()
        for i, row in enumerate(data):
            data[i] = prune_empties(row)
        self.assertListEqual(data, NEW_JOINT_CALLED_EXPECTED_VARIANT_AND_GENOTYPES_DATA)
        

    @mock.patch('lib.model.gcnv_mt_schema.datetime', wraps=datetime)
    def test_run_merged_tsv_task(self, mock_datetime):
        mock_datetime.date.today.return_value = datetime.date(2022, 12, 2)
        worker = luigi.worker.Worker()
        variant_task = SeqrGCNVVariantMTTask(
            source_paths=MERGED_CALLSET,
            dest_path=self._variant_mt_file,
        )
        genotype_task = SeqrGCNVGenotypesMTTask(
            genome_version="38",
            source_paths="i am completely ignored",
            dest_path=self._genotypes_mt_file,
            is_new_joint_call=False,
        )
        SeqrGCNVGenotypesMTTask.requires = lambda self: [variant_task]
        worker.add(genotype_task)
        worker.run()

        variant_mt = hl.read_matrix_table(self._variant_mt_file)
        self.assertEqual(variant_mt.count(), (2, 3))

        key_dropped_variant_mt = variant_mt.rows().flatten().drop("variant_name", "svtype")
        data = key_dropped_variant_mt.collect()
        self.assertCountEqual(data, MERGED_EXPECTED_VARIANT_DATA)

        genotypes_mt = hl.read_matrix_table(self._genotypes_mt_file)
        self.assertEqual(genotypes_mt.count(), (2, 3))

        # Now mimic the join in BaseMTToESOptimizedTask
        genotypes_mt = genotypes_mt.drop(*[k for k in genotypes_mt.globals.keys()])
        row_ht = genotypes_mt.rows().join(variant_mt.rows()).flatten().drop("variant_name", "svtype")
        data = row_ht.collect()
        for i, row in enumerate(data):
            data[i] = prune_empties(row)
        self.assertListEqual(data, MERGED_EXPECTED_VARIANT_AND_GENOTYPES_DATA)
