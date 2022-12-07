#! python
# Usage:
# $ python -m unittest discover -s <path-to>/sv_pipeline/exome -t <path-to>/sv_pipeline/exome -p *tests* in <path-to>/sv_pipeline/exome

import mock
import datetime
import sys
import unittest
import copy

from sv_pipeline.exome.load_data import main

NEW_JOINT_TSV_DATA = [
'chr	start	end	name	sample	sample_fix	svtype	GT	CN	NP	QA	QS	QSE	QSS	ploidy	strand	variant_name	ID	rmsstd	defragmented	vaf	vac	lt100_raw_calls	lt10_highQS_rare_calls	PASS_SAMPLE	PASS_FREQ	PASS_QS	HIGH_QUALITY	genes_any_overlap	genes_any_overlap_exonsPerGene	genes_any_overlap_totalExons	genes_strict_overlap	genes_strict_overlap_exonsPerGene	genes_strict_overlap_totalExons	genes_CG	genes_LOF	genes_any_overlap_Ensemble_ID	genes_LOF_Ensemble_ID	genes_CG_Ensemble_ID	var_source	callset_ovl	identical_ovl	partial_0_5_ovl	any_ovl	no_ovl	strvctvre_score\n',
'chr1	100006937	100007881	COHORT_1_Y_cnv_16326	RP-2037_PIE_OGI2271_003780_D1_v1_Exome_GCP	PIE_OGI2271_003780_D1_v1_Exome_RP-2037	DEL	1	1	1	4	4	4	4	2	-	suffix_16453	115884	0	FALSE	0.00004401408	1	TRUE	TRUE	TRUE	TRUE	FALSE	FALSE	AC118553.2,SLC35A3	1,1	2	None	0	0	NA	AC118553.2,SLC35A3	ENSG00000283761.1,ENSG00000117620.15	ENSG00000283761.1,ENSG00000117620.15		round2	round1	cluster_6_CASE_cnv_74577	cluster_6_CASE_cnv_74577	cluster_6_CASE_cnv_74577	FALSE	0.583\n',
'chr1	100017585	100023213	COHORT_13_Y_cnv_13436	C1981_PIE_OGI313_000747_1_v2_Exome_GCP	PIE_OGI313_000747_1_v2_Exome_C1981	DEL	1	1	2	4	5	5	4	2	-	suffix_16456	115888	0	FALSE	0.00008802817	2	FALSE	FALSE	FALSE	TRUE	FALSE	FALSE	AC118553.2,SLC35A3	1,2	3	None	0	0	NA	AC118553.2,SLC35A3	ENSG00000283761.1,ENSG00000117620.15	ENSG00000283761.1,ENSG00000117620.15		round2	round1	cluster_9_CASE_cnv_52542	cluster_9_CASE_cnv_52542	cluster_9_CASE_cnv_52542	FALSE	0.507\n',
'chr1	100017585	100023213	CASE_10_X_cnv_38690	C1992_MAN_0354_01_1_v1_Exome_GCP	MAN_0354_01_1_v1_Exome_C1992	DEL	1	0	2	20	30	30	20	2	-	suffix_16456	115887	0	FALSE	0.00008802817	2	FALSE	FALSE	FALSE	TRUE	FALSE	FALSE	AC118553.2,SLC35A3	1,2	3	None	0	0	NA	AC118553.2,SLC35A3	ENSG00000283761.1,ENSG00000117620.15	ENSG00000283761.1,ENSG00000117620.15		round2	round1	cluster_22_COHORT_cnv_56835	cluster_22_COHORT_cnv_56835	cluster_22_COHORT_cnv_56835	FALSE	0.507\n',
'chr1	100022289	100023213	COHORT_13_Y_cnv_25376	C1990_GLE-4772-4-2-a_v1_Exome_GCP	GLE-4772-4-2-a_v1_Exome_C1990	DEL	1	1	1	3	3	3	3	2	-	suffix_16457	115889	0	FALSE	0.00004401408	1	TRUE	TRUE	TRUE	TRUE	FALSE	FALSE	SLC35A3	1	1	None	0	0	NA	SLC35A3	ENSG00000117620.15	ENSG00000117620.15		round2	NA				TRUE	0.502\n',
]

MERGED_TSV_DATA = [
'chr	start	end	name	sample	sample_fix	svtype	GT	CN	NP	QA	QS	QSE	QSS	ploidy	strand	variant_name	ID	rmsstd	defragmented	vaf	vac	lt100_raw_calls	lt10_highQS_rare_calls	PASS_SAMPLE	PASS_FREQ	PASS_QS	HIGH_QUALITY	genes_any_overlap	genes_any_overlap_exonsPerGene	genes_any_overlap_totalExons	genes_strict_overlap	genes_strict_overlap_exonsPerGene	genes_strict_overlap_totalExons	genes_CG	genes_LOF	genes_any_overlap_Ensemble_ID	genes_LOF_Ensemble_ID	genes_CG_Ensemble_ID	var_source	callset_ovl	identical_ovl	partial_0_5_ovl	any_ovl	no_ovl	strvctvre_score	is_latest\n',
'chr1	100006937	100007881	COHORT_1_Y_cnv_16326	RP-2037_PIE_OGI2271_003780_D1_v1_Exome_GCP	PIE_OGI2271_003780_D1_v1_Exome_RP-2037	DEL	1	1	1	4	4	4	4	2	-	suffix_16453	115884	0	FALSE	4.401408e-05	1	TRUE	TRUE	TRUE	TRUE	FALSE	FALSE	AC118553.2,SLC35A3	1,1	2	None	0	0	NA	AC118553.2,SLC35A3	ENSG00000283761.1,ENSG00000117620.15	ENSG00000283761.1,ENSG00000117620.15		round2	round1	cluster_6_CASE_cnv_74577	cluster_6_CASE_cnv_74577	cluster_6_CASE_cnv_74577	FALSE	0.583	FALSE\n',
'chr1	100017585	100023213	COHORT_13_Y_cnv_13436	C1981_PIE_OGI313_000747_1_v2_Exome_GCP	PIE_OGI313_000747_1_v2_Exome_C1981	DEL	1	1	2	4	5	5	4	2	-	suffix_16456	115888	0	FALSE	8.802817e-05	2	FALSE	FALSE	FALSE	TRUE	FALSE	FALSE	AC118553.2,SLC35A3	1,2	3	None	0	0	NA	AC118553.2,SLC35A3	ENSG00000283761.1,ENSG00000117620.15	ENSG00000283761.1,ENSG00000117620.15		round2	round1	cluster_9_CASE_cnv_52542	cluster_9_CASE_cnv_52542	cluster_9_CASE_cnv_52542	FALSE	0.507	TRUE\n',
'chr1	100017585	100023213	CASE_10_X_cnv_38690	C1992_MAN_0354_01_1_v1_Exome_GCP	MAN_0354_01_1_v1_Exome_C1992	DEL	1	0	2	20	30	30	20	2	-	suffix_16456	115887	0	FALSE	8.802817e-05	2	FALSE	FALSE	FALSE	TRUE	FALSE	FALSE	AC118553.2,SLC35A3	1,2	3	None	0	0	NA	AC118553.2,SLC35A3	ENSG00000283761.1,ENSG00000117620.15	ENSG00000283761.1,ENSG00000117620.15		round2	round1	cluster_22_COHORT_cnv_56835	cluster_22_COHORT_cnv_56835	cluster_22_COHORT_cnv_56835	FALSE	0.507	FALSE\n',
]

EXPECTED_NEW_JOINT_CALLSET_ES_DATA = [
    {
        '_index': 'r0486_cmg_gcnv__structural_variants__wes__grch38__20221202',
        '_op_type': 'index',
        '_id': 'suffix_16453_DEL_12022022',
        '_source': {
            'contig': '1', 'sc': 1, 'sf': 4.401408e-05, 'svType': 'DEL', 'StrVCTVRE_score': 0.583,
            'variantId': 'suffix_16453_DEL_12022022', 'genotypes': [
                {'qs': 4, 'cn': 1, 'defragged': False, 'prev_overlap': True, 'new_call': False,
                 'prev_call': True, 'sample_id': 'PIE_OGI2271_003780_D1'}],
            'geneIds': mock.ANY, 'start': 100006937,
            'end': 100007881, 'num_exon': 2, 'transcriptConsequenceTerms': mock.ANY,
            'sn': 22720, 'pos': 100006937, 'xpos': 1100006937, 'xstart': 1100006937,
            'xstop': 1100007881, 'samples': ['PIE_OGI2271_003780_D1'], 'samples_new_call': [],
            'samples_cn_1': ['PIE_OGI2271_003780_D1'],
            'samples_qs_0_to_10': ['PIE_OGI2271_003780_D1'],
            'sortedTranscriptConsequences': mock.ANY
        }
    },
    {
        '_index': 'r0486_cmg_gcnv__structural_variants__wes__grch38__20221202', '_op_type': 'index',
        '_id': 'suffix_16456_DEL_12022022',
        '_source': {
            'contig': '1', 'sc': 2, 'sf': 8.802817e-05, 'svType': 'DEL', 'StrVCTVRE_score': 0.507,
            'variantId': 'suffix_16456_DEL_12022022',
            'genotypes': [
                {'qs': 5, 'cn': 1, 'defragged': False, 'prev_overlap': True, 'new_call': False,
                 'prev_call': True, 'sample_id': 'PIE_OGI313_000747_1'},
                {'qs': 30, 'cn': 0, 'defragged': False, 'prev_overlap': True, 'new_call': False,
                 'prev_call': True, 'sample_id': 'MAN_0354_01_1'}],
            'geneIds': mock.ANY, 'start': 100017585,
            'end': 100023213, 'num_exon': 3, 'transcriptConsequenceTerms': mock.ANY,
            'sn': 22719, 'pos': 100017585, 'xpos': 1100017585, 'xstart': 1100017585,
            'xstop': 1100023213, 'samples': ['PIE_OGI313_000747_1', 'MAN_0354_01_1'],
            'samples_new_call': [], 'samples_cn_1': ['PIE_OGI313_000747_1'],
            'samples_qs_0_to_10': ['PIE_OGI313_000747_1'], 'samples_cn_0': ['MAN_0354_01_1'],
            'samples_qs_30_to_40': ['MAN_0354_01_1'], 'sortedTranscriptConsequences': mock.ANY
        }
    },
    {
        '_index': 'r0486_cmg_gcnv__structural_variants__wes__grch38__20221202', '_op_type': 'index',
        '_id': 'suffix_16457_DEL_12022022',
        '_source': {
            'contig': '1', 'sc': 1, 'sf': 4.401408e-05, 'svType': 'DEL', 'StrVCTVRE_score': 0.502,
            'variantId': 'suffix_16457_DEL_12022022', 'genotypes': [
                {'qs': 3, 'cn': 1, 'defragged': False, 'prev_overlap': False, 'new_call': True,
                 'prev_call': False, 'sample_id': 'GLE-4772-4-2-a'}], 'geneIds': ['ENSG00000117620'],
            'start': 100022289, 'end': 100023213, 'num_exon': 1,
            'transcriptConsequenceTerms': mock.ANY, 'sn': 22720, 'pos': 100022289,
            'xpos': 1100022289, 'xstart': 1100022289, 'xstop': 1100023213,
            'samples': ['GLE-4772-4-2-a'], 'samples_new_call': ['GLE-4772-4-2-a'],
            'samples_cn_1': ['GLE-4772-4-2-a'], 'samples_qs_0_to_10': ['GLE-4772-4-2-a'],
            'sortedTranscriptConsequences': [
                {'gene_id': 'ENSG00000117620', 'major_consequence': 'LOF'}]
        }
    }
]

EXPECTED_MERGED_CALLSET_ES_DATA = [
    {
        '_index': 'r0486_cmg_gcnv__structural_variants__wes__grch38__20221202', '_op_type': 'index',
        '_id': 'suffix_16453_DEL_12022022',
        '_source': {
            'contig': '1', 'sc': 1, 'sf': 4.401408e-05, 'svType': 'DEL', 'StrVCTVRE_score': 0.583,
            'variantId': 'suffix_16453_DEL_12022022', 'genotypes': [
                {'qs': 4, 'cn': 1, 'defragged': False, 'prev_call': True, 'prev_overlap': False, 'new_call': False,
                 'sample_id': 'PIE_OGI2271_003780_D1'}],
            'geneIds': mock.ANY,
            'start': 100006937, 'end': 100007881, 'num_exon': 2,
            'transcriptConsequenceTerms': mock.ANY, 'sn': 22720, 'pos': 100006937,
            'xpos': 1100006937, 'xstart': 1100006937, 'xstop': 1100007881, 'samples': ['PIE_OGI2271_003780_D1'],
            'samples_new_call': [], 'samples_cn_1': ['PIE_OGI2271_003780_D1'],
            'samples_qs_0_to_10': ['PIE_OGI2271_003780_D1'],
            'sortedTranscriptConsequences': mock.ANY
        }
    },
    {
        '_index': 'r0486_cmg_gcnv__structural_variants__wes__grch38__20221202', '_op_type': 'index',
        '_id': 'suffix_16456_DEL_12022022',
        '_source': {
            'contig': '1', 'sc': 2, 'sf': 8.802817e-05, 'svType': 'DEL', 'StrVCTVRE_score': 0.507,
            'variantId': 'suffix_16456_DEL_12022022', 'genotypes': [
                {'qs': 5, 'cn': 1, 'defragged': False, 'prev_call': False, 'prev_overlap': False, 'new_call': False,
                 'sample_id': 'PIE_OGI313_000747_1'},
                {'qs': 30, 'cn': 0, 'defragged': False, 'prev_call': True, 'prev_overlap': False, 'new_call': False,
                 'sample_id': 'MAN_0354_01_1'}],
            'geneIds': mock.ANY, 'start': 100017585,
            'end': 100023213, 'num_exon': 3, 'transcriptConsequenceTerms': mock.ANY, 'sn': 22719,
            'pos': 100017585, 'xpos': 1100017585, 'xstart': 1100017585, 'xstop': 1100023213,
            'samples': ['PIE_OGI313_000747_1', 'MAN_0354_01_1'], 'samples_new_call': [],
            'samples_cn_1': ['PIE_OGI313_000747_1'], 'samples_qs_0_to_10': ['PIE_OGI313_000747_1'],
            'samples_cn_0': ['MAN_0354_01_1'], 'samples_qs_30_to_40': ['MAN_0354_01_1'],
            'sortedTranscriptConsequences': mock.ANY
        }
    }
]

EXPECTED_NEW_JOINT_CALLSET_ES_SCHEMA = \
    {
        'contig': {'type': 'keyword'}, 'sc': {'type': 'integer'},
        'sf': {'type': 'double'}, 'svType': {'type': 'keyword'},
        'StrVCTVRE_score': {'type': 'double'},
        'variantId': {'type': 'keyword'},
        'geneIds': {'type': 'keyword'}, 'start': {'type': 'integer'},
        'end': {'type': 'integer'}, 'num_exon': {'type': 'integer'},
        'transcriptConsequenceTerms': {'type': 'keyword'},
        'sn': {'type': 'integer'}, 'pos': {'type': 'integer'},
        'xpos': {'type': 'long'}, 'xstart': {'type': 'long'},
        'xstop': {'type': 'long'}, 'samples': {'type': 'keyword'},
        'samples_cn_1': {'type': 'keyword'},
        'samples_qs_0_to_10': {'type': 'keyword'},
        'samples_new_call': {'type': 'keyword'},
        'samples_cn_0': {'type': 'keyword'},
        'samples_qs_30_to_40': {'type': 'keyword'},
        'genotypes': {
            'type': 'nested',
            'properties': {
                'qs': {'type': 'integer'},
                'cn': {'type': 'integer'},
                'defragged': {'type': 'boolean'},
                'prev_overlap': {
                    'type': 'boolean'},
                'new_call': {'type': 'boolean'},
                'prev_call': {'type': 'boolean'},
                'sample_id': {'type': 'keyword'}}
        },
        'sortedTranscriptConsequences': {
            'type': 'nested',
            'properties': {
                'gene_id': {'type': 'keyword'},
                'major_consequence': {'type': 'keyword'}
            }
        }
    }

EXPECTED_MERGED_CALLSET_ES_SCHEMA = copy.deepcopy(EXPECTED_NEW_JOINT_CALLSET_ES_SCHEMA)
EXPECTED_MERGED_CALLSET_ES_SCHEMA.pop('samples_new_call')


def get_log_calls(num_recs, num_svs):
    return [
            mock.call('Parsing BED file'),
            mock.call(f'Found {num_recs} sample ids'),
            mock.call(f'Found {num_svs} SVs'),
            mock.call('\nFormatting for ES export'),
            mock.call(f'Exporting {num_svs} docs to ES index r0486_cmg_gcnv__structural_variants__wes__grch38__20221202'),
            mock.call('Deleting existing index'),
            mock.call('Setting up index'),
            mock.call('Starting bulk export'),
            mock.call(f'Successfully created {num_recs} records'),
            mock.call('DONE'),
        ]


class LoadDataTest(unittest.TestCase):

    def setUp(self):
        patcher = mock.patch('sv_pipeline.exome.load_data.date')
        mock_date = patcher.start()
        mock_date.today.return_value = datetime.date(2022, 12, 2)
        self.addCleanup(patcher.stop)
        patcher = mock.patch('sv_pipeline.utils.common.datetime')
        self.mock_date = patcher.start()
        self.mock_date.today.return_value.strftime.return_value = '20221202'
        self.addCleanup(patcher.stop)

    @mock.patch('sv_pipeline.exome.load_data.os')
    @mock.patch('sv_pipeline.exome.load_data.open')
    @mock.patch('sv_pipeline.exome.load_data.logger')
    @mock.patch('sv_pipeline.exome.load_data.es_helpers')
    @mock.patch('sv_pipeline.exome.load_data.ElasticsearchClient')
    def test_main(self, mock_es, mock_es_helpers, mock_logger, mock_open, mock_os):
        # test loading a new joint callset
        sys.argv[1:] = [
            'CMG_gCNV_2022_annotated.ensembl.round2_3.strvctvre.tsv',
            '--skip-sample-subset',
            '--is-new-joint-call',
            '--project-guid', 'R0486_cmg_gcnv',
        ]
        mock_os.environ.get.return_value = 'es_dummy_password'
        mock_f = mock_open.return_value.__enter__.return_value
        mock_f.readline.return_value = NEW_JOINT_TSV_DATA[0]
        mock_f.__iter__.return_value = NEW_JOINT_TSV_DATA[1:]
        mock_es_client = mock_es.return_value
        mock_es_client.es.indices.exists.return_value = True
        mock_es_helpers.bulk.return_value = (4, 0)
        main()
        mock_os.environ.get.assert_called_with('PIPELINE_ES_PASSWORD')
        mock_open.assert_called_with('CMG_gCNV_2022_annotated.ensembl.round2_3.strvctvre.tsv', 'r')
        self.assertEqual(mock_f.readline.call_count, 1)
        self.mock_date.today.return_value.strftime.assert_called_with('%Y%m%d')
        mock_logger.info.assert_has_calls(get_log_calls(num_recs=4, num_svs=3))
        mock_es_helpers.bulk.assert_called_with(mock_es_client.es, mock.ANY, chunk_size=1000)
        self.assertListEqual(EXPECTED_NEW_JOINT_CALLSET_ES_DATA, mock_es_helpers.bulk.call_args[0][1])
        mock_es.assert_called_with(host='localhost', port='9200', es_password='es_dummy_password')
        mock_es_client.es.indices.exists.assert_called_with(index='r0486_cmg_gcnv__structural_variants__wes__grch38__20221202')
        mock_es_client.es.indices.delete.assert_called_with(index='r0486_cmg_gcnv__structural_variants__wes__grch38__20221202')
        mock_es_client.create_index.assert_called_with(
            'r0486_cmg_gcnv__structural_variants__wes__grch38__20221202', mock.ANY, num_shards=1,
            _meta={'genomeVersion': '38', 'sampleType': 'WES', 'datasetType': 'SV',
                   'sourceFilePath': 'CMG_gCNV_2022_annotated.ensembl.round2_3.strvctvre.tsv'}
        )
        self.assertDictEqual(EXPECTED_NEW_JOINT_CALLSET_ES_SCHEMA, mock_es_client.create_index.call_args[0][1])

        # test loading a simply merged callset
        sys.argv[1:] = [
            'CMG_gCNV_2022_annotated.ensembl.round2_3.strvctvre.latest.tsv',
            '--skip-sample-subset',
            '--project-guid', 'R0486_cmg_gcnv',
        ]
        mock_logger.reset_mock()
        mock_es_helpers.reset_mock()
        mock_f.readline.return_value = MERGED_TSV_DATA[0]
        mock_f.__iter__.return_value = MERGED_TSV_DATA[1:]
        mock_es_helpers.bulk.return_value = (3, 0)
        main()
        mock_open.assert_called_with('CMG_gCNV_2022_annotated.ensembl.round2_3.strvctvre.latest.tsv', 'r')
        mock_logger.info.assert_has_calls(get_log_calls(num_recs=3, num_svs=2))
        mock_es_helpers.bulk.assert_called_with(mock_es_client.es, mock.ANY, chunk_size=1000)
        self.assertListEqual(EXPECTED_MERGED_CALLSET_ES_DATA, mock_es_helpers.bulk.call_args[0][1])
        mock_es_client.create_index.assert_called_with(
            'r0486_cmg_gcnv__structural_variants__wes__grch38__20221202', mock.ANY, num_shards=1,
            _meta={'genomeVersion': '38', 'sampleType': 'WES', 'datasetType': 'SV',
                   'sourceFilePath': 'CMG_gCNV_2022_annotated.ensembl.round2_3.strvctvre.latest.tsv'}
        )
        self.assertDictEqual(EXPECTED_MERGED_CALLSET_ES_SCHEMA, mock_es_client.create_index.call_args[0][1])
