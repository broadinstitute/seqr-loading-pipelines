import unittest

import hail as hl

from v03_pipeline.lib.misc.validation import (
    SeqrValidationError,
    validate_expected_contig_frequency,
    validate_imputed_sex_ploidy,
    validate_no_duplicate_variants,
    validate_sample_type,
)
from v03_pipeline.lib.model import ReferenceGenome, SampleType

TEST_SEX_CHECK_1 = 'v03_pipeline/var/test/sex_check/test_sex_check_1.ht'


def _mt_from_contigs(contigs):
    return hl.MatrixTable.from_parts(
        rows={
            'locus': [
                hl.Locus(
                    contig=contig,
                    position=1,
                    reference_genome='GRCh38',
                )
                for contig in contigs
            ],
        },
        cols={'s': ['sample_1']},
        entries={'HL': [[0.0] for _ in range(len(contigs))]},
    ).key_rows_by('locus')


class ValidationTest(unittest.TestCase):
    def test_validate_imputed_sex_ploidy(self) -> None:
        sex_check_ht = hl.read_table(TEST_SEX_CHECK_1)
        mt = hl.MatrixTable.from_parts(
            rows={
                'locus': [
                    hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                ],
            },
            cols={'s': ['HG00731_1', 'HG00732_1']},
            entries={
                'GT': [
                    [
                        hl.Call(alleles=[0, 0], phased=False),
                        hl.Call(alleles=[0], phased=False),
                    ],
                ],
            },
        ).key_rows_by('locus')
        validate_imputed_sex_ploidy(mt, sex_check_ht)
        mt = hl.MatrixTable.from_parts(
            rows={
                'locus': [
                    hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                ],
            },
            cols={'s': ['HG00731_1', 'HG00732_1']},
            entries={
                'GT': [
                    [
                        hl.Call(alleles=[0], phased=False),
                        hl.Call(alleles=[0], phased=False),
                    ],
                ],
            },
        ).key_rows_by('locus')
        self.assertRaisesRegex(
            SeqrValidationError,
            '50.00% of samples have misaligned ploidy',
            validate_imputed_sex_ploidy,
            mt,
            sex_check_ht,
        )


    def test_validate_no_duplicate_variants(self) -> None:
        mt = hl.MatrixTable.from_parts(
            rows={
                'locus': [
                    hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                ],
            },
            cols={'s': ['sample_1']},
            entries={'HL': [[0.0], [0.0], [0.0]]},
        ).key_rows_by('locus')
        self.assertRaisesRegex(
            SeqrValidationError,
            'Variants are present multiple times in the callset',
            validate_no_duplicate_variants,
            mt,
        )

    def test_validate_expected_contig_frequency(self) -> None:
        mt = _mt_from_contigs(ReferenceGenome.GRCh38.standard_contigs)
        self.assertIsNone(
            validate_expected_contig_frequency(mt, ReferenceGenome.GRCh38, 1),
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'Missing the following expected contigs',
            validate_expected_contig_frequency,
            mt,
            ReferenceGenome.GRCh37,
            1,
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'which is lower than expected minimum count',
            validate_expected_contig_frequency,
            mt,
            ReferenceGenome.GRCh38,
            2,
        )

        # Drop an optional contig
        mt = _mt_from_contigs(ReferenceGenome.GRCh38.standard_contigs - {'chrY'})
        self.assertIsNone(
            validate_expected_contig_frequency(mt, ReferenceGenome.GRCh38, 1),
        )

        # Drop a non-optional contig
        mt = _mt_from_contigs(ReferenceGenome.GRCh38.standard_contigs - {'chr3'})
        self.assertRaisesRegex(
            SeqrValidationError,
            'Missing the following expected contigs',
            validate_expected_contig_frequency,
            mt,
            ReferenceGenome.GRCh38,
            1,
        )

    def test_validate_sample_type(self) -> None:
        mt = _mt_from_contigs(ReferenceGenome.GRCh38.standard_contigs)
        coding_and_noncoding_variants_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'coding': True,
                    'noncoding': False,
                },
                {
                    'locus': hl.Locus(
                        contig='chr2',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'coding': True,
                    'noncoding': False,
                },
                {
                    'locus': hl.Locus(
                        contig='chr3',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'coding': False,
                    'noncoding': True,
                },
                {
                    'locus': hl.Locus(
                        contig='chr4',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'coding': False,
                    'noncoding': True,
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                coding=hl.tbool,
                noncoding=hl.tbool,
            ),
            key='locus',
        )
        self.assertIsNone(
            validate_sample_type(
                mt,
                coding_and_noncoding_variants_ht,
                ReferenceGenome.GRCh38,
                SampleType.WGS,
            ),
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'specified as WES but appears to be WGS',
            validate_sample_type,
            mt,
            coding_and_noncoding_variants_ht,
            ReferenceGenome.GRCh38,
            SampleType.WES,
        )

        # has coding, but not noncoding now.
        coding_and_noncoding_variants_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'coding': True,
                    'noncoding': False,
                },
                {
                    'locus': hl.Locus(
                        contig='chr2',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'coding': True,
                    'noncoding': False,
                },
                {
                    'locus': hl.Locus(
                        contig='chr2',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    'coding': False,
                    'noncoding': True,
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                coding=hl.tbool,
                noncoding=hl.tbool,
            ),
            key='locus',
        )
        self.assertIsNone(
            validate_sample_type(
                mt,
                coding_and_noncoding_variants_ht,
                ReferenceGenome.GRCh38,
                SampleType.WES,
            ),
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'specified as WGS but appears to be WES',
            validate_sample_type,
            mt,
            coding_and_noncoding_variants_ht,
            ReferenceGenome.GRCh38,
            SampleType.WGS,
        )

        # has noncoding, but not coding now.
        coding_and_noncoding_variants_ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'coding': False,
                    'noncoding': True,
                },
                {
                    'locus': hl.Locus(
                        contig='chr2',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'coding': False,
                    'noncoding': True,
                },
                {
                    'locus': hl.Locus(
                        contig='chr2',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    'coding': True,
                    'noncoding': False,
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                coding=hl.tbool,
                noncoding=hl.tbool,
            ),
            key='locus',
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'contains noncoding variants but is missing common coding variants',
            validate_sample_type,
            mt,
            coding_and_noncoding_variants_ht,
            ReferenceGenome.GRCh38,
            SampleType.WGS,
        )
