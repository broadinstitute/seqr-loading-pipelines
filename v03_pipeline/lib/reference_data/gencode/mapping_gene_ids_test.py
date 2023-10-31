import unittest
from unittest import mock

from v03_pipeline.lib.reference_data.gencode.mapping_gene_ids import load_gencode

DOWNLOAD_PATH = 'test/path'
GS_DOWNLOAD_PATH ='gs://test-bucket/test/path'
DOWNLOAD_FILE = 'test/path/gencode.v29.annotation.gtf.gz'
PICKLE_FILE = 'test/path/gencode.v29.annotation.gtf.pickle'
PICKLE_FILE_HANDLE = 'handle'
GTF_DATA = [
    '#description: evidence-based annotation of the human genome, version 31 (Ensembl 97), mapped to GRCh37 with gencode-backmap\n',
    'chr1   HAVANA  gene    11869   14409   .   +   .   gene_id "ENSG00000223972.5_2"; gene_type "transcribed_unprocessed_pseudogene"; gene_name "DDX11L1"; level 2; hgnc_id "HGNC:37102"; havana_gene "OTTHUMG00000000961.2_2"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "overlap";\n',
    'chr1   HAVANA  gene    621059  622053  .   -   .   gene_id "ENSG00000284662.1_2"; gene_type "protein_coding"; gene_name "OR4F16"; level 2; hgnc_id "HGNC:15079"; havana_gene "OTTHUMG00000002581.3_2"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "overlap";\n',
    'GL000193.1 HAVANA  gene    77815   78162   .   +   .   gene_id "ENSG00000279783.1_5"; gene_type "processed_pseudogene"; gene_name "AC018692.2"; level 2; havana_gene "OTTHUMG00000189459.1_5"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "new";\n',
]
GENE_ID_MAPPING = {"DDX11L1": "ENSG00000223972", "OR4F16": "ENSG00000284662", "AC018692.2": "ENSG00000279783"}


class LoadGencodeTestCase(unittest.TestCase):

    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.logger')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.path_exists')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.pickle')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.open')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.gzip.open')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.file_writer')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.download_file')
    def test_load_gencode_local(self, mock_download_file, mock_file_writer, mock_gopen, mock_open, mock_pickle,
                                mock_path_exists, mock_logger):
        # test using saved file
        mock_path_exists.side_effect = [True]
        mock_pickle.load.return_value = GENE_ID_MAPPING
        gene_id_mapping = load_gencode(23, download_path=DOWNLOAD_PATH)
        mock_file_writer.assert_not_called()
        mock_download_file.assert_not_called()
        mock_gopen.assert_not_called()
        mock_open.assert_called_with('test/path/gencode.v23.annotation.gtf.pickle', 'rb')
        mock_pickle.load.assert_called_with(mock_open.return_value.__enter__.return_value)
        mock_path_exists.assert_called_with('test/path/gencode.v23.annotation.gtf.pickle')
        mock_logger.info.assert_has_calls([
            mock.call('Use the existing pickle file test/path/gencode.v23.annotation.gtf.pickle.\nIf you want to reload the data, please delete it and re-run the data loading.'),
            mock.call('Got 3 gene id mapping records'),
        ])
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)

        # test downloading and parsing gtf data
        mock_path_exists.reset_mock()
        mock_logger.reset_mock()
        mock_pickle.reset_mock()
        mock_open.reset_mock()
        mock_path_exists.side_effect = [False, False]
        mock_download_file.return_value = 'test/path/gencode.v24.annotation.gtf.gz'
        mock_gopen.return_value.__iter__.return_value = GTF_DATA
        mock_f = mock.MagicMock()
        mock_file_writer.return_value.__enter__.return_value = mock_f, None
        gene_id_mapping = load_gencode(24, download_path=DOWNLOAD_PATH)
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)
        mock_path_exists.assert_has_calls([
            mock.call('test/path/gencode.v24.annotation.gtf.pickle'),
            mock.call('test/path/gencode.v24.annotation.gtf.gz'),
        ])
        mock_download_file.assert_called_with(
            'http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_24/gencode.v24.annotation.gtf.gz',
            to_dir='test/path',
        )
        mock_file_writer.assert_called_with('test/path/gencode.v24.annotation.gtf.pickle')
        mock_pickle.dump.assert_called_with(GENE_ID_MAPPING, mock_f, protocol=mock.ANY)
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
        mock_download_file.reset_mock()
        mock_pickle.reset_mock()
        mock_path_exists.side_effect = [False, True]
        mock_gopen.return_value.__iter__.return_value = GTF_DATA
        gene_id_mapping = load_gencode(24, download_path=DOWNLOAD_PATH)
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)
        mock_path_exists.assert_has_calls([
            mock.call('test/path/gencode.v24.annotation.gtf.pickle'),
            mock.call('test/path/gencode.v24.annotation.gtf.gz'),
        ])
        mock_gopen.assert_called_with('test/path/gencode.v24.annotation.gtf.gz', 'rt')
        mock_download_file.assert_not_called()
        mock_file_writer.assert_called_with('test/path/gencode.v24.annotation.gtf.pickle')
        mock_pickle.dump.assert_called_with(GENE_ID_MAPPING, mock_f, protocol=mock.ANY)
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

    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.gzip')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.logger')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.path_exists')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.pickle')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.stream_gs_file')
    @mock.patch('v03_pipeline.lib.reference_data.gencode.mapping_gene_ids.file_writer')
    def test_load_gencode_using_gs(self, mock_file_writer, mock_stream_gs_file, mock_pickle, mock_path_exists,
                                   mock_logger, mock_gzip):

        # test using saved file.
        mock_path_exists.side_effect = [True]
        mock_pickle.loads.return_value = GENE_ID_MAPPING
        gene_id_mapping = load_gencode(25, download_path=GS_DOWNLOAD_PATH)
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)
        mock_path_exists.assert_called_with('gs://test-bucket/test/path/gencode.v25.annotation.gtf.pickle')
        mock_logger.info.assert_has_calls([
            mock.call('Use the existing pickle file gs://test-bucket/test/path/gencode.v25.annotation.gtf.pickle.\n'
                      'If you want to reload the data, please delete it and re-run the data loading.'),
            mock.call('Got 3 gene id mapping records')
        ])
        mock_stream_gs_file.assert_called_with('gs://test-bucket/test/path/gencode.v25.annotation.gtf.pickle')
        mock_pickle.dump.assert_not_called()
        mock_file_writer.assert_not_called()

        # test using downloaded file.
        mock_path_exists.side_effect = [False, True]
        mock_gzip.decompress.return_value = ''.join(GTF_DATA).encode()
        mock_f = mock.MagicMock()
        mock_file_writer.return_value.__enter__.return_value = mock_f, None
        gene_id_mapping = load_gencode(25, download_path=GS_DOWNLOAD_PATH)
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)
        mock_path_exists.assert_has_calls([
            mock.call('gs://test-bucket/test/path/gencode.v25.annotation.gtf.pickle'),
            mock.call('gs://test-bucket/test/path/gencode.v25.annotation.gtf.gz'),
        ])
        mock_stream_gs_file.assert_called_with('gs://test-bucket/test/path/gencode.v25.annotation.gtf.gz', raw_download=True)
        mock_gzip.decompress.assert_called_with(mock_stream_gs_file.return_value)
        mock_file_writer.assert_called_with('gs://test-bucket/test/path/gencode.v25.annotation.gtf.pickle')
        mock_pickle.dump.assert_called_with(GENE_ID_MAPPING, mock_f, protocol=mock.ANY)