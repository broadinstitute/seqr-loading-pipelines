import shutil
import tempfile
from unittest.mock import ANY, Mock, patch

import hail as hl
import requests

from v03_pipeline.lib.misc.allele_registry import (
    HTTP_REQUEST_TIMEOUT_S as ALLELE_REGISTRY_TIMEOUT,
)
from v03_pipeline.lib.misc.allele_registry import (
    register_alleles,
    register_alleles_in_chunks,
)
from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SERVER_URL = 'http://reg.test.genome.network/alleles?file=vcf&fields=none+@id'


class AlleleRegistryTest(MockedDatarootTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        shutil.rmtree(self.temp_dir.name)

    @patch.object(requests, 'put')
    @patch(
        'v03_pipeline.lib.misc.allele_registry.get_ar_credentials_from_secret_manager',
    )
    @patch('v03_pipeline.lib.misc.allele_registry.Env')
    @patch('v03_pipeline.lib.misc.allele_registry.logger')
    def test_register_alleles_38(
        self,
        mock_logger: Mock,
        mock_env: Mock,
        mock_get_credentials: Mock,
        mock_put_request: Mock,
    ):
        mock_get_credentials.return_value = ('', '')
        mock_env.HAIL_TMPDIR = self.temp_dir.name

        new_variants_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=10126,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['TA', 'T'],
                    'rsid': 'rs370233999',
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=10129,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['T', 'TC'],
                    'rsid': 'rs370233997',
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=10128,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'G'],
                    'rsid': 'rs370234000',
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=10469,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['C', 'G'],
                    'rsid': 'rs370233998',
                },
            ],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
                rsid=hl.tstr,
            ),
            key=('locus', 'alleles'),
        )

        mock_response = Mock()
        mock_put_request.return_value = mock_response
        mock_response.ok = True
        mock_response.json.return_value = [
            {
                '@id': 'http://reg.genome.network/allele/CA997563840',
                'genomicAlleles': [
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': '',  # alt allele is ''
                                'end': 10128,
                                'referenceAllele': 'A',
                                'start': 10127,
                            },
                        ],
                        'referenceGenome': 'GRCh38',
                    },
                ],
                'externalRecords': {
                    'gnomAD_4': [{'id': '1-10126-TA-T'}],
                },  # has gnomad ID
            },
            {
                '@id': 'http://reg.genome.network/allele/CA16716503',
                'genomicAlleles': [
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'C',
                                'end': 10131,
                                'referenceAllele': '',  # ref allele is '' and does not have a gnomad ID
                                'start': 10131,
                            },
                        ],
                        'referenceGenome': 'GRCh38',
                    },
                ],
            },
            {
                '@id': 'http://reg.genome.network/allele/CA997563845',
                'genomicAlleles': [
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'G',
                                'end': 10128,
                                'referenceAllele': 'A',
                                'start': 10127,
                            },
                        ],
                        'referenceGenome': 'GRCh38',
                    },
                ],
                'externalRecords': {'gnomAD_4': [{'id': '1-10128-A-G'}]},
            },
            {
                'description': 'Given allele cannot be mapped in consistent way to reference genome.',
                'errorType': 'InternalServerError',
                'inputLine': 'Cannot align NC_000001.10 [10468,10469).',
                'message': '1	10469	rs370233998	C	G	.	.	.',
            },
        ]

        ar_ht = register_alleles(
            new_variants_ht,
            ReferenceGenome.GRCh38,
            TEST_SERVER_URL,
        )
        self.assertEqual(
            ar_ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus('chr1', 10126, 'GRCh38'),
                    alleles=['TA', 'T'],
                    CAID='CA997563840',
                ),
                hl.Struct(
                    locus=hl.Locus('chr1', 10128, 'GRCh38'),
                    alleles=['A', 'G'],
                    CAID='CA997563845',
                ),
            ],
        )
        mock_put_request.assert_called_once_with(
            url=ANY,
            data=f'{"".join(ReferenceGenome.GRCh38.allele_registry_vcf_header)}'
            f'1\t10126\trs370233999\tTA\tT\t.\t.\t.\n'
            f'1\t10128\trs370234000\tA\tG\t.\t.\t.\n'
            f'1\t10129\trs370233997\tT\tTC\t.\t.\t.\n'
            f'1\t10469\trs370233998\tC\tG\t.\t.\t.\n',
            timeout=ALLELE_REGISTRY_TIMEOUT,
        )
        mock_logger.warning.assert_called_once_with(
            '1 failed. First error: \n'
            'API URL: http://reg.test.genome.network/alleles?file=vcf&fields=none+@id\n'
            'TYPE: InternalServerError\n'
            'DESCRIPTION: Given allele cannot be mapped in consistent way to reference genome.\n'
            'MESSAGE: 1\t10469\trs370233998\tC\tG\t.\t.\t.\n'
            'INPUT_LINE: Cannot align NC_000001.10 [10468,10469).',
        )

    @patch('v03_pipeline.lib.misc.allele_registry.register_alleles')
    def test_register_alleles_in_chunks(self, mock_register_alleles):
        chunk_size = 10
        ht = hl.Table.parallelize(
            [{'x': x} for x in range(chunk_size * 3 + 5)],  # 35 rows, expect 4 chunks
            hl.tstruct(x=hl.tint32),
            key='x',
        )

        # Instead of actually calling register_alleles, capture and assert on
        # the value of 'x' in the first row of each chunk and number of rows in each chunk
        def _side_effect(chunk_ht: hl.Table, *_):
            value_in_first_row = hl.eval(chunk_ht.take(1)[0].x)
            num_rows_in_chunk = chunk_ht.count()
            return value_in_first_row, num_rows_in_chunk

        mock_register_alleles.side_effect = _side_effect
        generator = register_alleles_in_chunks(
            ht=ht,
            reference_genome=ReferenceGenome.GRCh38,
            base_url=TEST_SERVER_URL,
            chunk_size=chunk_size,
        )
        self.assertEqual(list(generator), [(0, 10), (10, 10), (20, 10), (30, 5)])

    def test_register_alleles_in_chunks_no_new_variants(self):
        ht = hl.Table.parallelize(
            [],
            hl.tstruct(x=hl.tint32),
            key='x',
        )
        empty_generator = register_alleles_in_chunks(
            ht=ht,
            reference_genome=ReferenceGenome.GRCh38,
            base_url=TEST_SERVER_URL,
        )
        with self.assertRaises(StopIteration):
            next(empty_generator)
