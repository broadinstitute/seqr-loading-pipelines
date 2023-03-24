import datetime
import shutil
import tempfile
import unittest
from unittest import mock

import hail as hl
import luigi.worker

from lib.model.gcnv_mt_schema import parse_genes, hl_agg_collect_set_union
from seqr_gcnv_loading import SeqrGCNVVariantMTTask, SeqrGCNVGenotypesMTTask, SeqrGCNVMTToESTask

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
        sortedTranscriptConsequences=[
            {'gene_id': 'ENSG00000117620', 'major_consequence': 'LOF'},
            {'gene_id': 'ENSG00000283761', 'major_consequence': 'LOF'}
        ],
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
        geneIds=['ENSG00000117620', 'ENSG00000283761', 'ENSG22222222222'],
        sortedTranscriptConsequences=[
            {'gene_id': 'ENSG00000117620', 'major_consequence': 'LOF'},
            {'gene_id': 'ENSG00000283761', 'major_consequence': 'LOF'}, 
            {'gene_id': 'ENSG22222222222'}
        ],
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
            hl.Struct(**{'cn': 0,
                'defragged': False,
                'start': 100017586,
                'end': 100023212,
                'geneIds': ['ENSG00000283761', 'ENSG22222222222'],
                'new_call': False,
                'num_exon': 2,
                'prev_call': True,
                'prev_overlap': False,
                'qs': 30,
                'sample_id': 'BEN_0234_01_1'
            }),
            hl.Struct(**{
                'geneIds': ['ENSG00000117620', 'ENSG00000283761'],
                'qs': 30,
                'cn': 0, 
                'defragged': False, 
                'prev_overlap': False, 
                'new_call': False,
                'prev_call': True,
                'sample_id': 'MAN_0354_01_1'
            }),
            hl.Struct(**{
                'geneIds': ['ENSG00000117620', 'ENSG00000283761'],
                'qs': 5,
                'cn': 1, 
                'defragged': False, 
                'prev_overlap': False, 
                'new_call': False,
                'prev_call': False,
                'sample_id':  'PIE_OGI313_000747_1'
            }),
        ],
        samples=['BEN_0234_01_1', 'MAN_0354_01_1', 'PIE_OGI313_000747_1'], 
        **{
            'samples_cn.1': ['PIE_OGI313_000747_1'],
            'samples_qs.0_to_10': ['PIE_OGI313_000747_1'], 
            'samples_cn.0': ['BEN_0234_01_1', 'MAN_0354_01_1'],
            'samples_qs.30_to_40': ['BEN_0234_01_1', 'MAN_0354_01_1'],
        },
    ),
]
MERGED_EXPECTED_VARIANT_AND_GENOTYPES_DATA = [
    hl.Struct(**x, **y) for (x, y) in zip(MERGED_EXPECTED_VARIANT_DATA, MERGED_EXPECTED_GENOTYPES_DATA)
]

EXPECTED_DISABLED_INDEX_FIELDS = ['contig', 'genotypes', 'start', 'xstart', 'variantId']

def prune_empties(data):
    if isinstance(data, list):
        data = [prune_empties(x) for x in data if x is not None]
    elif isinstance(data, hl.Struct):
        data = hl.Struct(**{k : prune_empties(v) for k, v in data.items() if v is not None})
    elif isinstance(data, dict):
        data = {k: prune_empties(v) for k, v in data.items() if v is not None}
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
        SeqrGCNVMTToESTask.disable_instance_cache()

    def tearDown(self):
        shutil.rmtree(self._temp_dir.name)

    @mock.patch('lib.model.gcnv_mt_schema.datetime', wraps=datetime)
    @mock.patch('lib.hail_tasks.HailElasticsearchClient')
    def test_run_new_joint_tsv_task(self, mock_elasticsearch_client, mock_datetime):
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
        export_task = SeqrGCNVMTToESTask()
        SeqrGCNVGenotypesMTTask.requires = lambda self: [variant_task]
        SeqrGCNVMTToESTask.requires = lambda self: [variant_task, genotype_task]
        worker.add(export_task)
        worker.run()

        variant_mt = hl.read_matrix_table(self._variant_mt_file)
        self.assertEqual(variant_mt.count(), (3, 4))

        key_dropped_variant_mt = variant_mt.rows().flatten().drop("variant_name", "svtype")
        data = key_dropped_variant_mt.collect()
        self.assertCountEqual(data, NEW_JOINT_CALLED_EXPECTED_VARIANT_DATA)

        # Genotypes (only) Assertions
        genotypes_mt = hl.read_matrix_table(self._genotypes_mt_file)
        self.assertEqual(genotypes_mt.count(), (3, 4))

        export_task._es.export_table_to_elasticsearch.assert_called_once()
        args, kwargs = export_task._es.export_table_to_elasticsearch.call_args
        row_ht = args[0].collect()
        row_ht = prune_empties(row_ht)
        self.assertCountEqual(row_ht, NEW_JOINT_CALLED_EXPECTED_VARIANT_AND_GENOTYPES_DATA)
        self.assertCountEqual(kwargs["disable_index_for_fields"], EXPECTED_DISABLED_INDEX_FIELDS)

    @mock.patch('lib.model.gcnv_mt_schema.datetime', wraps=datetime)
    @mock.patch('lib.hail_tasks.HailElasticsearchClient')
    def test_run_merged_tsv_task(self, mock_elasticsearch_client, mock_datetime):
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
        export_task = SeqrGCNVMTToESTask()
        SeqrGCNVGenotypesMTTask.requires = lambda self: [variant_task]
        SeqrGCNVMTToESTask.requires = lambda self: [variant_task, genotype_task]
        worker.add(export_task)
        worker.run()

        variant_mt = hl.read_matrix_table(self._variant_mt_file)
        self.assertEqual(variant_mt.count(), (2, 4))

        key_dropped_variant_mt = variant_mt.rows().flatten().drop("variant_name", "svtype")
        data = key_dropped_variant_mt.collect()
        self.assertCountEqual(data, MERGED_EXPECTED_VARIANT_DATA)

        genotypes_mt = hl.read_matrix_table(self._genotypes_mt_file)
        self.assertEqual(genotypes_mt.count(), (2, 4))

        export_task._es.export_table_to_elasticsearch.assert_called_once()
        args, kwargs = export_task._es.export_table_to_elasticsearch.call_args
        row_ht = args[0].collect()
        row_ht = prune_empties(row_ht)
        self.assertCountEqual(row_ht, MERGED_EXPECTED_VARIANT_AND_GENOTYPES_DATA)
        self.assertCountEqual(kwargs["disable_index_for_fields"], EXPECTED_DISABLED_INDEX_FIELDS)
