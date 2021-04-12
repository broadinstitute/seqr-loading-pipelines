import unittest
import mock

from sv_pipeline.genome.utils.mapping_gene_ids import load_gencode, GENOME_VERSION_GRCh37, GENOME_VERSION_GRCh38

GTF_FILE = 'test/path/test.gtf.gz'
GTF_FILE_LIFT = 'test/path/test.lift.gtf.gz'
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
    def test_load_gencode(self, mock_download, mock_isfile, mock_logger):

        with self.assertRaises(Exception) as ee:
            load_gencode(19, gencode_gtf_path=GTF_FILE, genome_version=GENOME_VERSION_GRCh38)
        self.assertEqual(str(ee.exception), 'Invalid genome_version: {}. gencode v19 only has a GRCh37 version'.format(GENOME_VERSION_GRCh38))

        with self.assertRaises(Exception) as ee:
            load_gencode(22, gencode_gtf_path=GTF_FILE, genome_version=GENOME_VERSION_GRCh37)
        self.assertEqual(str(ee.exception), 'Invalid genome_version: {}. gencode v20, v21, v22 only have a GRCh38 version'.format(GENOME_VERSION_GRCh37))

        with self.assertRaises(Exception) as ee:
            load_gencode(23, gencode_gtf_path=GTF_FILE, genome_version=GENOME_VERSION_GRCh37)
        self.assertEqual(str(ee.exception), "Invalid genome_version for file: {}. gencode v23 and up must have 'lift' in the filename or genome_version arg must be GRCh38".format(GTF_FILE))

        with self.assertRaises(Exception) as ee:
            load_gencode(23, gencode_gtf_path=GTF_FILE)
        self.assertEqual(str(ee.exception), "The genome version must also be specified after the gencode GTF file path")

        mock_isfile.return_value = True
        with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data=''.join(GTF_DATA))) as mock_open:
            gene_id_mapping = load_gencode(23, gencode_gtf_path=GTF_FILE, genome_version=GENOME_VERSION_GRCh38)
        self.assertEqual(gene_id_mapping, GENE_ID_MAPPING)
        mock_isfile.assert_called_with(GTF_FILE)
        mock_open.assert_called_with(GTF_FILE, 'rt')
        calls = [
            mock.call("Loading {} (genome version: {})".format(GTF_FILE, GENOME_VERSION_GRCh38)),
            mock.call('Get 3 gene id mapping records'),
        ]
        mock_logger.info.assert_has_calls(calls)

        with self.assertRaises(ValueError) as ve:
            with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data='bad data')) as mock_open:
                load_gencode(23, gencode_gtf_path=GTF_FILE, genome_version=GENOME_VERSION_GRCh38)
        mock_open.assert_called_with(GTF_FILE, 'rt')
        self.assertEqual(str(ve.exception), "Unexpected number of fields on line #0: ['bad data']")

        # tests for downloading .gtf files
        mock_logger.reset_mock()
        mock_isfile.return_value = False
        mock_download.return_value = GTF_FILE
        with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data=''.join(GTF_DATA))) as mock_open:
            load_gencode(19)
        mock_download.assert_called_with('http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_19/gencode.v19.annotation.gtf.gz')
        mock_open.assert_called_with(GTF_FILE, 'rt')
        mock_logger.info.assert_has_calls([mock.call("Loading {} (genome version: {})".format(GTF_FILE, GENOME_VERSION_GRCh37))])

        mock_logger.reset_mock()
        with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data=''.join(GTF_DATA))) as mock_open:
            load_gencode(22)
        mock_download.assert_called_with('http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_22/gencode.v22.annotation.gtf.gz')
        mock_open.assert_called_with(GTF_FILE, 'rt')
        mock_logger.info.assert_has_calls([mock.call("Loading {} (genome version: {})".format(GTF_FILE, GENOME_VERSION_GRCh38))])

        mock_logger.reset_mock()
        mock_download.reset_mock()
        mock_download.side_effect = [GTF_FILE_LIFT, GTF_FILE]
        with mock.patch('sv_pipeline.genome.utils.mapping_gene_ids.gzip.open', mock.mock_open(read_data=''.join(GTF_DATA))) as mock_open:
            load_gencode(29)
        calls = [
            mock.call('http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/GRCh37_mapping/gencode.v29lift37.annotation.gtf.gz'),
            mock.call('http://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_29/gencode.v29.annotation.gtf.gz')
        ]
        mock_download.assert_has_calls(calls)
        mock_open.assert_called_with(GTF_FILE, 'rt')
        calls = [
            mock.call("Loading {} (genome version: {})".format(GTF_FILE_LIFT, GENOME_VERSION_GRCh37)),
            mock.call("Loading {} (genome version: {})".format(GTF_FILE, GENOME_VERSION_GRCh38)),
        ]
        mock_logger.info.assert_has_calls(calls)
