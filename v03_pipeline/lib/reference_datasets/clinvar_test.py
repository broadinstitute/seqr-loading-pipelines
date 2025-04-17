import unittest

import hail as hl
import responses

from v03_pipeline.lib.annotations.enums import (
    CLINVAR_ASSERTIONS,
    CLINVAR_PATHOGENICITIES,
)
from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.clinvar import (
    parsed_and_mapped_clnsigconf,
    parsed_clnsig,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset
from v03_pipeline.lib.test.mock_clinvar_urls import mock_clinvar_urls


class ClinvarTest(unittest.TestCase):
    @responses.activate
    def test_get_clinvar_version(self):
        with mock_clinvar_urls():
            self.assertEqual(
                ReferenceDataset.clinvar.version(ReferenceGenome.GRCh38),
                '2024-11-11',
            )

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

    @responses.activate
    def test_get_ht(self):
        with mock_clinvar_urls():
            ht = ReferenceDataset.clinvar.get_ht(
                ReferenceGenome.GRCh38,
            )
            self.assertEqual(
                ht.globals.collect()[0],
                hl.Struct(
                    version='2024-11-11',
                    enums=hl.Struct(
                        assertion=CLINVAR_ASSERTIONS,
                        pathogenicity=CLINVAR_PATHOGENICITIES,
                    ),
                ),
            )
            self.assertEqual(
                ht.collect()[:3],
                [
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=69134,
                            reference_genome='GRCh38',
                        ),
                        alleles=['A', 'G'],
                        alleleId=2193183,
                        conflictingPathogenicities=None,
                        goldStars=1,
                        submitters=None,
                        conditions=None,
                        pathogenicity_id=0,
                        assertion_ids=[],
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=69314,
                            reference_genome='GRCh38',
                        ),
                        alleles=['T', 'G'],
                        alleleId=3374047,
                        conflictingPathogenicities=None,
                        goldStars=1,
                        submitters=['Paris Brain Institute, Inserm - ICM', 'OMIM'],
                        conditions=[
                            'Hereditary spastic paraplegia 48',
                            'Hereditary spastic paraplegia 48',
                        ],
                        pathogenicity_id=12,
                        assertion_ids=[],
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=69423,
                            reference_genome='GRCh38',
                        ),
                        alleles=['G', 'A'],
                        alleleId=3374048,
                        conflictingPathogenicities=None,
                        goldStars=1,
                        submitters=['OMIM'],
                        conditions=['Hereditary spastic paraplegia 48'],
                        pathogenicity_id=12,
                        assertion_ids=[],
                    ),
                ],
            )

            # VariationID 9 tests Conditions parsing
            self.assertListEqual(
                ht.collect()[8].submitters,
                [
                    'Hemochromatosis type 1',
                    'Hereditary cancer-predisposing syndrome',
                    'HFE-related disorder',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Bronze diabetes',
                    'Hemochromatosis type 1',
                    'HFE-related disorder',
                    'Hemochromatosis type 1',
                    'Abdominal pain',
                    'Atypical behavior',
                    'Pain',
                    'Peripheral neuropathy',
                    'Abnormality of the nervous system',
                    'Abnormality of the male genitalia',
                    'Abnormal peripheral nervous system morphology',
                    'Hereditary hemochromatosis',
                    'Hemochromatosis type 1',
                    'not provided',
                    'Hereditary hemochromatosis',
                    'not provided',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'not provided',
                    'not provided',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hereditary hemochromatosis',
                    'Cardiomyopathy',
                    'not provided',
                    'Juvenile hemochromatosis',
                    'Hemochromatosis type 1',
                    'not provided',
                    'not provided',
                    'Inborn genetic diseases',
                    'Hemochromatosis type 1',
                    'not provided',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'not provided',
                    'Porphyrinuria',
                    'Cutaneous photosensitivity',
                    'Hemochromatosis type 1',
                    'Hereditary hemochromatosis',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                    'not provided',
                    'Hemochromatosis type 1',
                    'Variegate porphyria',
                    'Familial porphyria cutanea tarda',
                    'Alzheimer disease type 1',
                    'Microvascular complications of diabetes, susceptibility to, 7',
                    'Transferrin serum level quantitative trait locus 2',
                    'Hemochromatosis type 1',
                    'Hemochromatosis type 1',
                ],
            )
