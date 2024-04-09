from unittest.mock import Mock, patch

import requests

from v03_pipeline.lib.misc.allele_registry import register_alleles
from v03_pipeline.lib.test.mocked_dataroot_testcase import MockedDatarootTestCase

TEST_SERVER_URL = 'http://reg.test.genome.network/alleles?file=hgvs'
VALID_HGVS_EXPRESSIONS = [
    'ENST00000450305.2:n.182+58G>A',
    'ENST00000327044.6:c.1667C>T',
    'ENST00000477976.5:n.3114C>T',
]
INVALID_HGVS_EXPRESSIONS = [
    'ENST00000387314.1:n.1G>C',
    'ENST00000387314.1:n.51dup',
    'ENST00000387314.1:n.51_52insA',
]


class AlleleRegistryTest(MockedDatarootTestCase):
    @patch.object(requests, 'put')
    @patch('v03_pipeline.lib.misc.allele_registry.logger')
    def test_allele_registry(
        self,
        mock_logger: Mock,
        mock_put_request: Mock,
    ):
        self.mock_env.ALLELE_REGISTRY_LOGIN = ''
        self.mock_env.ALLELE_REGISTRY_PASSWORD = ''
        mock_response = Mock()
        mock_put_request.return_value = mock_response
        mock_response.ok = True
        mock_response.json.return_value = [
            {
                '@context': 'http://reg.genome.network/schema/allele.jsonld',
                '@id': 'http://reg.genome.network/allele/CA887895211',
                'communityStandardTitle': ['NC_000001.11:g.12755G>A'],
                'externalRecords': {},
                'genomicAlleles': [
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'A',
                                'end': 12755,
                                'referenceAllele': 'G',
                                'start': 12754,
                            },
                        ],
                        'hgvs': ['NC_000001.11:g.12755G>A', 'CM000663.2:g.12755G>A'],
                        'referenceGenome': 'GRCh38',
                        'referenceSequence': 'http://reg.genome.network/refseq/RS000049',
                    },
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'A',
                                'end': 2618,
                                'referenceAllele': 'G',
                                'start': 2617,
                            },
                        ],
                        'hgvs': ['NC_000001.9:g.2618G>A'],
                        'referenceGenome': 'NCBI36',
                        'referenceSequence': 'http://reg.genome.network/refseq/RS000001',
                    },
                ],
                'transcriptAlleles': [],
                'type': 'nucleotide',
            },
            {
                '@context': 'http://reg.genome.network/schema/allele.jsonld',
                '@id': 'http://reg.genome.network/allele/CA503883',
                'communityStandardTitle': [
                    'NM_015658.4(NOC2L):c.1667C>T (p.Ser556Leu)',
                ],
                'externalRecords': {},
                'genomicAlleles': [
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'A',
                                'end': 946538,
                                'referenceAllele': 'G',
                                'start': 946537,
                            },
                        ],
                        'hgvs': ['NC_000001.11:g.946538G>A', 'CM000663.2:g.946538G>A'],
                        'referenceGenome': 'GRCh38',
                        'referenceSequence': 'http://reg.genome.network/refseq/RS000049',
                    },
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'A',
                                'end': 881918,
                                'referenceAllele': 'G',
                                'start': 881917,
                            },
                        ],
                        'hgvs': ['NC_000001.10:g.881918G>A', 'CM000663.1:g.881918G>A'],
                        'referenceGenome': 'GRCh37',
                        'referenceSequence': 'http://reg.genome.network/refseq/RS000025',
                    },
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'A',
                                'end': 871781,
                                'referenceAllele': 'G',
                                'start': 871780,
                            },
                        ],
                        'hgvs': ['NC_000001.9:g.871781G>A'],
                        'referenceGenome': 'NCBI36',
                        'referenceSequence': 'http://reg.genome.network/refseq/RS000001',
                    },
                ],
                'transcriptAlleles': [],
                'type': 'nucleotide',
            },
            {
                '@context': 'http://reg.genome.network/schema/allele.jsonld',
                '@id': 'http://reg.genome.network/allele/CA503883',
                'communityStandardTitle': [
                    'NM_015658.4(NOC2L):c.1667C>T (p.Ser556Leu)',
                ],
                'externalRecords': {},
                'genomicAlleles': [
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'A',
                                'end': 946538,
                                'referenceAllele': 'G',
                                'start': 946537,
                            },
                        ],
                        'hgvs': ['NC_000001.11:g.946538G>A', 'CM000663.2:g.946538G>A'],
                        'referenceGenome': 'GRCh38',
                        'referenceSequence': 'http://reg.genome.network/refseq/RS000049',
                    },
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'A',
                                'end': 881918,
                                'referenceAllele': 'G',
                                'start': 881917,
                            },
                        ],
                        'hgvs': ['NC_000001.10:g.881918G>A', 'CM000663.1:g.881918G>A'],
                        'referenceGenome': 'GRCh37',
                        'referenceSequence': 'http://reg.genome.network/refseq/RS000025',
                    },
                    {
                        'chromosome': '1',
                        'coordinates': [
                            {
                                'allele': 'A',
                                'end': 871781,
                                'referenceAllele': 'G',
                                'start': 871780,
                            },
                        ],
                        'hgvs': ['NC_000001.9:g.871781G>A'],
                        'referenceGenome': 'NCBI36',
                        'referenceSequence': 'http://reg.genome.network/refseq/RS000001',
                    },
                ],
                'transcriptAlleles': [],
                'type': 'nucleotide',
            },
            {
                'description': 'Internal error occurred. Please, report it as an error.',
                'errorType': 'InternalServerError',
                'inputLine': 'ENST00000387314.1:n.1G>C',
                'message': 'Unknown reference: ENST00000387314.1',
            },
            {
                'description': 'Internal error occurred. Please, report it as an error.',
                'errorType': 'InternalServerError',
                'inputLine': 'ENST00000387314.1:n.51dup',
                'message': 'Unknown reference: ENST00000387314.1',
            },
            {
                'description': 'Internal error occurred. Please, report it as an error.',
                'errorType': 'InternalServerError',
                'inputLine': 'ENST00000387314.1:n.51_52insA',
                'message': 'Unknown reference: ENST00000387314.1',
            },
        ]

        register_alleles(
            VALID_HGVS_EXPRESSIONS + INVALID_HGVS_EXPRESSIONS,
            TEST_SERVER_URL,
        )

        mock_logger.warning.assert_called_once_with(
            '3 alleles failed to register to http://reg.test.genome.network/alleles?file=hgvs. ENST00000387314.1:n.1G>C: Unknown reference: ENST00000387314.1, ENST00000387314.1:n.51dup: Unknown reference: ENST00000387314.1, ENST00000387314.1:n.51_52insA: Unknown reference: ENST00000387314.1',
        )
