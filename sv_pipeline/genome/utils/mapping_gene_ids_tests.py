import unittest
import mock

from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode, _load_parsed_data_or_download, _parse_gtf_data

DOWNLOAD_PATH = 'test/path'
DOWNLOAD_FILE = 'test/path/gencode.v29.annotation.gtf.gz'
PICKLE_FILE = 'test/path/gencode.v29.annotation.gtf.pickle'
PICKLE_FILE_HANDLE = 'handle'
GTF_DATA = [
    '#description: evidence-based annotation of the human genome, version 31 (Ensembl 97), mapped to GRCh37 with gencode-backmap\n',
    'chr1	HAVANA	gene	11869	14409	.	+	.	gene_id "ENSG00000223972.5_2"; gene_type "transcribed_unprocessed_pseudogene"; gene_name "DDX11L1"; level 2; hgnc_id "HGNC:37102"; havana_gene "OTTHUMG00000000961.2_2"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "overlap";\n',
    'chr1	HAVANA	gene	621059	622053	.	-	.	gene_id "ENSG00000284662.1_2"; gene_type "protein_coding"; gene_name "OR4F16"; level 2; hgnc_id "HGNC:15079"; havana_gene "OTTHUMG00000002581.3_2"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "overlap";\n',
    'GL000193.1	HAVANA	gene	77815	78162	.	+	.	gene_id "ENSG00000279783.1_5"; gene_type "processed_pseudogene"; gene_name "AC018692.2"; level 2; havana_gene "OTTHUMG00000189459.1_5"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "new";\n',
]
GENE_ID_MAPPING = {"DDX11L1": "ENSG00000223972", "OR4F16": "ENSG00000284662", "AC018692.2": "ENSG00000279783"}


class LoadGencodeTestCase(unittest.TestCase):

    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.logger')
    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.path_exists')
    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.pickle')
    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.open')
    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open')
    @mock.patch('sv_pipeline.genome.utils.download_utils.open')
    def test_load_gencode(self, mock_download_open, mock_gopen, mock_open, mock_pickle, mock_path_exists, mock_logger):
        # test using saved file
        mock_path_exists.side_effect = [True]
        mock_pickle.load.return_value = GENE_ID_MAPPING
        gene_id_mapping = load_gencode(23, download_path=DOWNLOAD_PATH)
        mock_download_open.assert_not_called()
        mock_gopen.assert_not_called()
        mock_open.assert_called_with('test/path/gencode.v23.annotation.gtf.pickle', 'rb')
        mock_pickle.load.assert_called_with(mock_open.return_value.__enter__.return_value)
        mock_path_exists.assert_called_with('test/path/gencode.v23.annotation.gtf.pickle')
        mock_logger.info.assert_has_calls([
            mock.call('Use the existing pickle file test/path/gencode.v23.annotation.gtf.pickle.\nIf you want to reload the data, please delete it and re-run the data loading.'),
            mock.call('Got 3 gene id mapping records'),
        ])
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)

        # test parsing gtf data
        mock_path_exists.reset_mock()
        mock_logger.reset_mock()
        mock_pickle.reset_mock()
        mock_open.reset_mock()
        mock_path_exists.side_effect = [False, False]
        mock_gopen.return_value.__iter__.return_value = GTF_DATA
        gene_id_mapping = load_gencode(24, download_path=DOWNLOAD_PATH)
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)
        mock_path_exists.assert_has_calls([
            mock.call('test/path/gencode.v24.annotation.gtf.pickle'),
            mock.call('test/path/gencode.v24.annotation.gtf.gz'),
        ])
        mock_download_open.assert_has_calls([
            mock.call('test/path/gencode.v24.annotation.gtf.gz', 'wb'),
            mock.call().writelines(mock.ANY),
            mock.call().close(),
            mock.call('test/path/gencode.v24.annotation.gtf.pickle', 'wb'),
            mock.call().close(),
        ])
        mock_gopen.assert_called_with('test/path/gencode.v24.annotation.gtf.gz', 'rt')
        mock_open.assert_not_called()
        mock_logger.info.assert_has_calls([
            mock.call('Downloaded to test/path/gencode.v24.annotation.gtf.gz'),
            mock.call('Loading test/path/gencode.v24.annotation.gtf.gz'),
            mock.call('Saving to pickle test/path/gencode.v24.annotation.gtf.pickle'),
            mock.call('Got 3 gene id mapping records')
        ])
        mock_pickle.load.assert_not_called()

        # test using downloaded file
        mock_path_exists.reset_mock()
        mock_logger.reset_mock()
        mock_download_open.reset_mock()
        mock_path_exists.side_effect = [False, True]
        mock_gopen.return_value.__iter__.return_value = GTF_DATA
        gene_id_mapping = load_gencode(24, download_path=DOWNLOAD_PATH)
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)
        mock_path_exists.assert_has_calls([
            mock.call('test/path/gencode.v24.annotation.gtf.pickle'),
            mock.call('test/path/gencode.v24.annotation.gtf.gz'),
        ])
        mock_gopen.assert_called_with('test/path/gencode.v24.annotation.gtf.gz', 'rt')
        mock_download_open.assert_has_calls([
            mock.call('test/path/gencode.v24.annotation.gtf.pickle', 'wb'),
            mock.call().close(),
        ])
        mock_open.assert_not_called()
        mock_logger.info.assert_has_calls([
            mock.call('Use the existing downloaded file test/path/gencode.v24.annotation.gtf.gz.\nIf you want to re-download it, please delete the file and re-run the pipeline.'),
            mock.call('Loading test/path/gencode.v24.annotation.gtf.gz'),
            mock.call('Saving to pickle test/path/gencode.v24.annotation.gtf.pickle'),
            mock.call('Got 3 gene id mapping records')
        ])
        mock_pickle.load.assert_not_called()

        # bad gtf data test
        mock_path_exists.side_effect = [False, False]
        mock_gopen.return_value.__iter__.return_value = ['bad data']
        with self.assertRaises(ValueError) as ve:
            _ = load_gencode(24, download_path=DOWNLOAD_PATH)
        self.assertEqual(str(ve.exception), "Unexpected number of fields on line #0: ['bad data']")
