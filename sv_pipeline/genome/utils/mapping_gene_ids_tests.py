import unittest
import mock
import hail as hl

from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode

GTF_FILE = 'test/path/test.gtf.gz'
PICKLE_FILE = 'test/path/test.gtf.pickle'
PICKLE_FILE_HANDLE = 'handle'
GTF_DATA = [
    '#description: evidence-based annotation of the human genome, version 31 (Ensembl 97), mapped to GRCh37 with gencode-backmap\n',
    'chr1	HAVANA	gene	11869	14409	.	+	.	gene_id "ENSG00000223972.5_2"; gene_type "transcribed_unprocessed_pseudogene"; gene_name "DDX11L1"; level 2; hgnc_id "HGNC:37102"; havana_gene "OTTHUMG00000000961.2_2"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "overlap";\n',
    'chr1	HAVANA	gene	621059	622053	.	-	.	gene_id "ENSG00000284662.1_2"; gene_type "protein_coding"; gene_name "OR4F16"; level 2; hgnc_id "HGNC:15079"; havana_gene "OTTHUMG00000002581.3_2"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "overlap";\n',
    'GL000193.1	HAVANA	gene	77815	78162	.	+	.	gene_id "ENSG00000279783.1_5"; gene_type "processed_pseudogene"; gene_name "AC018692.2"; level 2; havana_gene "OTTHUMG00000189459.1_5"; remap_status "full_contig"; remap_num_mappings 1; remap_target_status "new";\n',
]
GENE_ID_MAPPING = {"DDX11L1": "ENSG00000223972.5_2", "OR4F16": "ENSG00000284662.1_2", "AC018692.2": "ENSG00000279783.1_5"}


class LoadGencodeTestCase(unittest.TestCase):

    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.logger')
    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.os.path.isfile')
    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.download_file')
    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.pickle')
    @mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.open')
    def test_load_gencode(self, mock_open, mock_pickle, mock_download, mock_isfile, mock_logger):
        mock_isfile.return_value = True
        mock_pickle.load.return_value = GENE_ID_MAPPING
        load_gencode(23, gencode_gtf_path=GTF_FILE)
        mock_isfile.assert_called_with(PICKLE_FILE)
        mock_open.assert_called_with(PICKLE_FILE, 'rb')
        mock_pickle.load.assert_called_with(mock_open.return_value)

        mock_isfile.reset_mock()
        mock_isfile.side_effect = [True, False]
        with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data=''.join(GTF_DATA))) as mock_gopen:
            load_gencode(23, gencode_gtf_path=GTF_FILE)
        mock_isfile.assert_called_with(PICKLE_FILE)
        mock_open.assert_called_with(PICKLE_FILE, 'wb')
        mock_gopen.assert_called_with(GTF_FILE, 'rt')
        calls = [
            mock.call("Loading {}".format(GTF_FILE)),
            mock.call('Get 3 gene id mapping records'),
        ]
        mock_logger.info.assert_has_calls(calls)

        mock_isfile.reset_mock()
        mock_isfile.side_effect = [True, False]
        with self.assertRaises(ValueError) as ve:
            with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data='bad data')) as mock_gopen:
                load_gencode(23, gencode_gtf_path=GTF_FILE)
        mock_gopen.assert_called_with(GTF_FILE, 'rt')
        self.assertEqual(str(ve.exception), "Unexpected number of fields on line #0: ['bad data']")

        # tests for downloading .gtf files
        mock_logger.reset_mock()
        mock_isfile.reset_mock()
        mock_isfile.side_effect = [False, False]
        mock_download.return_value = GTF_FILE
        with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data=''.join(GTF_DATA))) as mock_gopen:
            load_gencode(29, download_path='test')
        mock_download.assert_called_with('http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/gencode.v29.annotation.gtf.gz', to_dir='test')
        mock_gopen.assert_called_with(GTF_FILE, 'rt')
        mock_logger.info.assert_has_calls(calls)

        with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data=''.join(GTF_DATA))) as mock_gopen:
            load_gencode(29)
        mock_download.assert_called_with('http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/gencode.v29.annotation.gtf.gz', to_dir=None)
        mock_gopen.assert_called_with(GTF_FILE, 'rt')
