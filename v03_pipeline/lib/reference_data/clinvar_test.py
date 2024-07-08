import unittest
from unittest import mock

import hail as hl

from v03_pipeline.lib.reference_data.clinvar import (
    import_submission_table,
    join_to_submission_summary_ht,
    parsed_and_mapped_clnsigconf,
    parsed_clnsig,
)


class ClinvarTest(unittest.TestCase):
    def test_parsed_clnsig(self):
        ht = hl.Table.parallelize(
            [
                {'info': hl.Struct(CLNSIG=['Pathogenic|Affects'])},
                {
                    'info': hl.Struct(
                        CLNSIG=[
                            'Pathogenic/Likely_pathogenic/Pathogenic',
                            '_low_penetrance',
                        ],
                    ),
                },
                {
                    'info': hl.Struct(
                        CLNSIG=[
                            'Likely_pathogenic/Pathogenic',
                            '_low_penetrance|association|protective',
                        ],
                    ),
                },
                {'info': hl.Struct(CLNSIG=['Likely_pathogenic', '_low_penetrance'])},
                {'info': hl.Struct(CLNSIG=['association|protective'])},
                {
                    'info': hl.Struct(
                        CLNSIG=[
                            'Pathogenic/Likely_pathogenic/Pathogenic',
                            '_low_penetrance/Established_risk_allele',
                        ],
                    ),
                },
            ],
            hl.tstruct(info=hl.tstruct(CLNSIG=hl.tarray(hl.tstr))),
        )
        self.assertListEqual(
            parsed_clnsig(ht).collect(),
            [
                ['Pathogenic', 'Affects'],
                ['Pathogenic/Likely_pathogenic', 'low_penetrance'],
                ['Likely_pathogenic', 'low_penetrance', 'association', 'protective'],
                ['Likely_pathogenic', 'low_penetrance'],
                ['association', 'protective'],
                [
                    'Pathogenic/Likely_pathogenic/Established_risk_allele',
                    'low_penetrance',
                ],
            ],
        )

    def test_parsed_and_mapped_clnsigconf(self):
        ht = hl.Table.parallelize(
            [
                {'info': hl.Struct(CLNSIGCONF=hl.missing(hl.tarray(hl.tstr)))},
                {
                    'info': hl.Struct(
                        CLNSIGCONF=[
                            'Pathogenic(8)|Likely_pathogenic(2)|Pathogenic',
                            '_low_penetrance(1)|Uncertain_significance(1)',
                        ],
                    ),
                },
            ],
            hl.tstruct(info=hl.tstruct(CLNSIGCONF=hl.tarray(hl.tstr))),
        )
        self.assertListEqual(
            parsed_and_mapped_clnsigconf(ht).collect(),
            [
                None,
                [
                    hl.Struct(count=9, pathogenicity_id=0),
                    hl.Struct(count=2, pathogenicity_id=5),
                    hl.Struct(count=1, pathogenicity_id=12),
                ],
            ],
        )

    @mock.patch(
        'v03_pipeline.lib.reference_data.clinvar.hl.import_table',
    )
    def test_import_submission_table(self, mock_import_table):
        mock_import_table.return_value = hl.Table.parallelize(
            [
                {
                    '#VariationID': '5',
                    'Submitter': 'OMIM',
                    'ReportedPhenotypeInfo': 'C3661900:not provided',
                },
                {
                    '#VariationID': '5',
                    'Submitter': 'Broad Institute Rare Disease Group, Broad Institute',
                    'ReportedPhenotypeInfo': 'C0023264:Leigh syndrome',
                },
                {
                    '#VariationID': '5',
                    'Submitter': 'PreventionGenetics, part of Exact Sciences',
                    'ReportedPhenotypeInfo': 'na:FOXRED1-related condition',
                },
                {
                    '#VariationID': '5',
                    'Submitter': 'Invitae',
                    'ReportedPhenotypeInfo': 'C4748791:Mitochondrial complex 1 deficiency, nuclear type 19',
                },
                {
                    '#VariationID': '6',
                    'Submitter': 'A',
                    'ReportedPhenotypeInfo': 'na:B',
                },
            ],
        )
        ht = import_submission_table('mock_file_name')
        self.assertEqual(
            ht.collect(),
            [
                hl.Struct(
                    VariationID='5',
                    Submitters=[
                        'OMIM',
                        'Broad Institute Rare Disease Group, Broad Institute',
                        'PreventionGenetics, part of Exact Sciences',
                        'Invitae',
                    ],
                    Conditions=[
                        'C3661900:not provided',
                        'C0023264:Leigh syndrome',
                        'na:FOXRED1-related condition',
                        'C4748791:Mitochondrial complex 1 deficiency, nuclear type 19',
                    ],
                ),
                hl.Struct(
                    VariationID='6',
                    Submitters=['A'],
                    Conditions=['na:B'],
                ),
            ],
        )

    @mock.patch(
        'v03_pipeline.lib.reference_data.clinvar.hl.read_table',
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.clinvar.download_import_write_submission_summary',
    )
    def test_join_to_submission_summary_ht(
        self,
        mock_download,
        mock_read_existing_table,
    ):
        vcf_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'C'],
                    'rsid': '5',
                    'info': hl.Struct(ALLELEID=1),
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'AC'],
                    'rsid': '7',
                    'info': hl.Struct(ALLELEID=1),
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
                rsid=hl.tstr,
                info=hl.tstruct(ALLELEID=hl.tint32),
            ),
        )
        submitters_ht = hl.Table.parallelize(
            [
                {
                    'VariationID': '5',
                    'Submitters': [
                        'OMIM',
                        'Broad Institute Rare Disease Group, Broad Institute',
                        'PreventionGenetics, part of Exact Sciences',
                        'Invitae',
                    ],
                    'Conditions': [
                        'C3661900:not provided',
                        'C0023264:Leigh syndrome',
                        'na:FOXRED1-related condition',
                        'C4748791:Mitochondrial complex 1 deficiency, nuclear type 19',
                    ],
                },
                {'VariationID': '6', 'Submitters': ['A'], 'Conditions': ['na:B']},
            ],
            hl.tstruct(
                VariationID=hl.tstr,
                Submitters=hl.tarray(hl.tstr),
                Conditions=hl.tarray(hl.tstr),
            ),
            key='VariationID',
        )
        expected_clinvar_ht_rows = [
            hl.Struct(
                locus=hl.Locus(
                    contig='chr1',
                    position=871269,
                    reference_genome='GRCh38',
                ),
                alleles=['A', 'C'],
                rsid='5',
                info=hl.Struct(ALLELEID=1),
                submitters=[
                    'OMIM',
                    'Broad Institute Rare Disease Group, Broad Institute',
                    'PreventionGenetics, part of Exact Sciences',
                    'Invitae',
                ],
                conditions=[
                    'C3661900:not provided',
                    'C0023264:Leigh syndrome',
                    'na:FOXRED1-related condition',
                    'C4748791:Mitochondrial complex 1 deficiency, nuclear type 19',
                ],
            ),
            hl.Struct(
                locus=hl.Locus(
                    contig='chr1',
                    position=871269,
                    reference_genome='GRCh38',
                ),
                alleles=['A', 'AC'],
                rsid='7',
                info=hl.Struct(ALLELEID=1),
                submitters=None,
                conditions=None,
            ),
        ]

        # Tests reading cached submission summary table
        mock_read_existing_table.return_value = submitters_ht
        ht = join_to_submission_summary_ht(vcf_ht)
        self.assertEqual(
            ht.collect(),
            expected_clinvar_ht_rows,
        )
        mock_download.assert_not_called()

        # Tests downloading new submission summary table
        mock_read_existing_table.side_effect = hl.utils.FatalError('Table not found')
        mock_download.return_value = submitters_ht
        ht = join_to_submission_summary_ht(vcf_ht)
        self.assertEqual(
            ht.collect(),
            expected_clinvar_ht_rows,
        )
