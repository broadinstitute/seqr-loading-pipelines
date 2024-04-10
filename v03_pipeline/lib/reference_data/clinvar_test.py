import unittest
from unittest import mock

import hail as hl

from v03_pipeline.lib.reference_data.clinvar import (
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
        'v03_pipeline.lib.reference_data.clinvar.download_and_import_clinvar_submission_summary',
    )
    def test_join_to_submission_summary_ht(self, mock_download):
        clinvar_enums_struct = hl.Struct(
            CLNSIG=[
                'Pathogenic/Likely_pathogenic/Pathogenic',
                '_low_penetrance',
            ],
            CLNSIGCONF=[
                'Pathogenic(8)|Likely_pathogenic(2)|Pathogenic',
                '_low_penetrance(1)|Uncertain_significance(1)',
            ],
            CLNREVSTAT=['no_classifications_from_unflagged_records'],
        )
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
                    'info': hl.Struct(ALLELEID=1, **clinvar_enums_struct),
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    'alleles': ['A', 'AC'],
                    'rsid': '7',
                    'info': hl.Struct(ALLELEID=1, **clinvar_enums_struct),
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
                rsid=hl.tstr,
                info=hl.tstruct(
                    ALLELEID=hl.tint32,
                    CLNSIG=hl.tarray(hl.tstr),
                    CLNSIGCONF=hl.tarray(hl.tstr),
                    CLNREVSTAT=hl.tarray(hl.tstr),
                ),
            ),
        )
        mock_download.return_value = hl.Table.parallelize(
            [
                {
                    '#VariationID': '5',
                    'Submitter': 'OMIM',
                    'ClinicalSignificance': 'Pathogenic',
                    'ReportedPhenotypeInfo': 'C3661900:not provided',
                },
                {
                    '#VariationID': '5',
                    'Submitter': 'Broad Institute Rare Disease Group, Broad Institute',
                    'ClinicalSignificance': 'Uncertain significance',
                    'ReportedPhenotypeInfo': 'C0023264:Leigh syndrome',
                },
                {
                    '#VariationID': '5',
                    'Submitter': 'PreventionGenetics, part of Exact Sciences',
                    'ClinicalSignificance': 'Likely benign',
                    'ReportedPhenotypeInfo': 'na:FOXRED1-related condition',
                },
                {
                    '#VariationID': '5',
                    'Submitter': 'Invitae',
                    'ClinicalSignificance': 'not provided',
                    'ReportedPhenotypeInfo': 'C4748791:Mitochondrial complex 1 deficiency, nuclear type 19',
                },
                {
                    '#VariationID': '6',
                    'Submitter': 'A',
                    'ClinicalSignificance': None,
                    'ReportedPhenotypeInfo': 'na:B',
                },
            ],
            hl.tstruct(
                **{
                    '#VariationID': hl.tstr,
                    'Submitter': hl.tstr,
                    'ClinicalSignificance': hl.tstr,
                    'ReportedPhenotypeInfo': hl.tstr,
                },
            ),
        )
        ht = join_to_submission_summary_ht(vcf_ht)
        self.assertEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome='GRCh38',
                    ),
                    alleles=['A', 'C'],
                    rsid='5',
                    info=hl.Struct(ALLELEID=1, **clinvar_enums_struct),
                    submitters=[
                        'OMIM',
                        'Broad Institute Rare Disease Group, Broad Institute',
                        'PreventionGenetics, part of Exact Sciences',
                        'Invitae',
                    ],
                    clinical_significances=[
                        'Pathogenic',
                        'Uncertain significance',
                        'Likely benign',
                        'not provided',
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
                    info=hl.Struct(ALLELEID=1, **clinvar_enums_struct),
                    submitters=None,
                    clinical_significances=None,
                    conditions=None,
                ),
            ],
        )
