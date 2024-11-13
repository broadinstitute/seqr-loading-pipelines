import unittest

import hail as hl
import responses

from v03_pipeline.lib.annotations.enums import (
    CLINVAR_ASSERTIONS,
    CLINVAR_PATHOGENICITIES,
)
from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.clinvar import (
    CLINVAR_SUBMISSION_SUMMARY_URL,
    parsed_and_mapped_clnsigconf,
    parsed_clnsig,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset

CLINVAR_VCF = 'v03_pipeline/var/test/reference_data/clinvar.vcf.gz'
CLINVAR_SUBMISSION_SUMMARY = (
    'v03_pipeline/var/test/reference_data/submission_summary.txt.gz'
)


class ClinvarTest(unittest.TestCase):
    @responses.activate
    def test_get_clinvar_version(self):
        with open(CLINVAR_VCF, 'rb') as f:
            responses.get(
                ReferenceDataset.clinvar.raw_dataset_path(ReferenceGenome.GRCh37),
                body=f.read(),
            )
            self.assertEqual(
                ReferenceDataset.clinvar.version(ReferenceGenome.GRCh37),
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
        with open(CLINVAR_VCF, 'rb') as f1, open(
            CLINVAR_SUBMISSION_SUMMARY,
            'rb',
        ) as f2:
            responses.get(
                ReferenceDataset.clinvar.raw_dataset_path(ReferenceGenome.GRCh38),
                body=f1.read(),
            )
            responses.get(
                CLINVAR_SUBMISSION_SUMMARY_URL,
                body=f2.read(),
            )
            responses.add_passthru('http://localhost')
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
                        pathogenicity_id=14,
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
