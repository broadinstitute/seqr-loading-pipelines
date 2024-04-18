from unittest.mock import ANY, Mock, patch

import hail as hl
import requests

from v03_pipeline.lib.misc.allele_registry import (
    HTTP_REQUEST_TIMEOUT as ALLELE_REGISTRY_TIMEOUT,
)
from v03_pipeline.lib.misc.allele_registry import (
    register_alleles,
    register_alleles_in_chunks,
)
from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SERVER_URL = 'http://reg.test.genome.network/alleles?file=vcf&fields=none+@id'


class AlleleRegistryTest(MockedDatarootTestCase):
    @patch.object(requests, 'put')
    @patch('v03_pipeline.lib.misc.allele_registry.Env')
    @patch('v03_pipeline.lib.misc.allele_registry.logger')
    def test_register_alleles_38(
        self,
        mock_logger: Mock,
        mock_env: Mock,
        mock_put_request: Mock,
    ):
        mock_env.ALLELE_REGISTRY_LOGIN = 'test'
        mock_env.ALLELE_REGISTRY_PASSWORD = 'test'  # noqa: S105

        new_variants_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=874734,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['C', 'T'],
                    'rsid': 'rs370233997',
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=876499,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'G'],
                    'rsid': 'rs370233999',
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=878314,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['G', 'C'],
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
            {'@id': 'http://reg.genome.network/allele/CA997563840'},
            {'@id': 'http://reg.genome.network/allele/CA16716503'},
            {'@id': 'http://reg.genome.network/allele/CA997563845'},
            {
                'description': 'Given allele cannot be mapped in consistent way to reference genome.',
                'errorType': 'InternalServerError',
                'inputLine': 'Cannot align NC_000001.10 [10468,10469).',
                'message': '1	10469	rs370233998	C	G	.	.	.',
            },
        ]

        register_alleles(new_variants_ht, ReferenceGenome.GRCh38, TEST_SERVER_URL)
        mock_put_request.assert_called_once_with(
            url=ANY,
            data=f'{"".join(ReferenceGenome.GRCh38.allele_registry_vcf_header)}'
            f'1\t10469\trs370233998\tC\tG\t.\t.\t.\n'
            f'1\t874734\trs370233997\tC\tT\t.\t.\t.\n'
            f'1\t876499\trs370233999\tA\tG\t.\t.\t.\n'
            f'1\t878314\trs370234000\tG\tC\t.\t.\t.\n',
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
        first_row_values = []
        num_rows_per_chunk = []

        def _get_ht_info(chunk_ht: hl.Table, *_) -> None:
            first_row_values.append(hl.eval(chunk_ht.take(1)[0].x))
            num_rows_per_chunk.append(chunk_ht.count())

        mock_register_alleles.side_effect = _get_ht_info
        register_alleles_in_chunks(
            ht,
            ReferenceGenome.GRCh38,
            TEST_SERVER_URL,
            chunk_size,
        )
        self.assertEqual(4, mock_register_alleles.call_count)
        self.assertEqual(first_row_values, [0, 10, 20, 30])
        self.assertEqual(num_rows_per_chunk, [10, 10, 10, 5])
