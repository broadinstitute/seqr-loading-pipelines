import unittest

import hail as hl

from v03_pipeline.lib.misc.validation import (
    SeqrValidationError,
    validate_allele_depth_length,
    validate_allele_type,
    validate_expected_contig_frequency,
    validate_imported_field_types,
    validate_imputed_sex_ploidy,
    validate_no_duplicate_variants,
    validate_sample_type,
)
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, SampleType

TEST_SEX_CHECK_1 = 'v03_pipeline/var/test/sex_check/test_sex_check_1.ht'
TEST_MITO_MT = 'v03_pipeline/var/test/callsets/mito_1.mt'


def _mt_from_contigs(contigs):
    return (
        hl.MatrixTable.from_parts(
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
        )
        .key_rows_by('locus')
        .key_cols_by('s')
    )


class ValidationTest(unittest.TestCase):
    def test_validate_allele_type(self) -> None:
        mt = (
            hl.MatrixTable.from_parts(
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
                            position=3,
                            reference_genome='GRCh38',
                        ),
                        hl.Locus(
                            contig='chr1',
                            position=4,
                            reference_genome='GRCh38',
                        ),
                    ],
                    'alleles': [
                        ['A', 'T'],
                        # NB: star alleles should pass through this validation just fine,
                        # but are eventually filtered out upstream.
                        ['A', '*'],
                        ['A', '-'],
                        ['A', '<NON_REF>'],
                    ],
                },
                cols={'s': ['sample_1']},
                entries={'HL': [[0.0], [0.0], [0.0], [0.0]]},
            )
            .key_rows_by('locus', 'alleles')
            .key_cols_by('s')
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            "Alleles with invalid AlleleType are present in the callset: \\[\\('A', '-'\\), \\('A', '<NON_REF>'\\)\\]",
            validate_allele_type,
            mt,
            DatasetType.SNV_INDEL,
        )

        mt = (
            hl.MatrixTable.from_parts(
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
                    ],
                    'alleles': [
                        ['C', '<NON_REF>'],
                        ['A', '<NON_REF>'],
                    ],
                },
                cols={'s': ['sample_1']},
                entries={'HL': [[0.0], [0.0]]},
            )
            .key_rows_by('locus', 'alleles')
            .key_cols_by('s')
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            'Alleles with invalid allele <NON_REF> are present in the callset.  This appears to be a GVCF containing records for sites with no variants.',
            validate_allele_type,
            mt,
            DatasetType.SNV_INDEL,
        )

    def test_validate_allele_depth_length(self) -> None:
        mt = (
            hl.MatrixTable.from_parts(
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
                            position=3,
                            reference_genome='GRCh38',
                        ),
                        hl.Locus(
                            contig='chr1',
                            position=4,
                            reference_genome='GRCh38',
                        ),
                    ],
                    'alleles': [
                        ['A', 'T'],
                        # NB: star alleles should pass through this validation just fine,
                        # but are eventually filtered out upstream.
                        ['A', 'TC', 'TG'],
                        ['A', 'TTT'],
                        ['A', 'CCC'],
                    ],
                },
                cols={'s': ['sample_1', 'sample_2']},
                entries={
                    'AD': [
                        [[1, 0], [1, 0]],
                        [[1], [1, 0, 1]],
                        [[1, 0], [1]],
                        [[1, 0], [1, 0]],
                    ],
                },
            )
            .key_rows_by('locus', 'alleles')
            .key_cols_by('s')
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            "Found variants with unequal Allele Depth array lengths over samples \\(first 10, if applicable\\): \\{'1-2-A-TC-TG': \\{1, 3\\}, '1-3-A-TTT': \\{1, 2\\}\\}",
            validate_allele_depth_length,
            mt,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
        )

    def test_validate_imputed_sex_ploidy(self) -> None:
        female_sample = 'HG00731_1'
        male_sample_1 = 'HG00732_1'
        male_sample_2 = 'HG00732_1'
        x0_sample = 'NA20899_1'
        xxy_sample = 'NA20889_1'
        xyy_sample = 'NA20891_1'
        xxx_sample = 'NA20892_1'

        sex_check_ht = hl.read_table(TEST_SEX_CHECK_1)

        # All calls on X chromosome are valid
        mt = (
            hl.MatrixTable.from_parts(
                rows={
                    'locus': [
                        hl.Locus(
                            contig='chrX',
                            position=1,
                            reference_genome='GRCh38',
                        ),
                    ],
                },
                cols={
                    's': [
                        female_sample,
                        male_sample_1,
                        x0_sample,
                        xxy_sample,
                        xyy_sample,
                        xxx_sample,
                    ],
                },
                entries={
                    'GT': [
                        [
                            hl.Call(alleles=[0, 0], phased=False),
                            hl.Call(alleles=[0], phased=False),
                            hl.Call(alleles=[0, 0], phased=False),  # X0
                            hl.Call(alleles=[0, 0], phased=False),  # XXY
                            hl.Call(alleles=[0, 0], phased=False),  # XYY
                            hl.Call(alleles=[0, 0], phased=False),  # XXX
                        ],
                    ],
                },
            )
            .key_rows_by('locus')
            .key_cols_by('s')
        )
        validate_imputed_sex_ploidy(mt, sex_check_ht)

        # All calls on Y chromosome are valid
        mt = (
            hl.MatrixTable.from_parts(
                rows={
                    'locus': [
                        hl.Locus(
                            contig='chrY',
                            position=1,
                            reference_genome='GRCh38',
                        ),
                    ],
                },
                cols={
                    's': [
                        female_sample,
                        male_sample_1,
                        x0_sample,
                        xxy_sample,
                        xyy_sample,
                        xxx_sample,
                    ],
                },
                entries={
                    'GT': [
                        [
                            hl.missing(hl.tcall),
                            hl.Call(alleles=[0], phased=False),
                            hl.missing(hl.tcall),  # X0
                            hl.Call(alleles=[0, 0], phased=False),  # XXY
                            hl.Call(alleles=[0, 0], phased=False),  # XYY
                            hl.missing(hl.tcall),  # XXX
                        ],
                    ],
                },
            )
            .key_rows_by('locus')
            .key_cols_by('s')
        )
        validate_imputed_sex_ploidy(mt, sex_check_ht)

        # Invalid X chromosome case
        mt = (
            hl.MatrixTable.from_parts(
                rows={
                    'locus': [
                        hl.Locus(
                            contig='chrX',
                            position=1,
                            reference_genome='GRCh38',
                        ),
                    ],
                },
                cols={
                    's': [
                        female_sample,
                        male_sample_1,
                        male_sample_2,
                        x0_sample,
                        xxy_sample,
                        xyy_sample,
                        xxx_sample,
                    ],
                },
                entries={
                    'GT': [
                        [
                            hl.Call(alleles=[0], phased=False),  # invalid Female call
                            hl.Call(alleles=[0], phased=False),  # valid Male call
                            hl.missing(hl.tcall),  # invalid Male call
                            hl.Call(alleles=[0], phased=False),  # invalid X0 call
                            hl.Call(alleles=[0], phased=False),  # invalid XXY call
                            hl.missing(hl.tcall),  # valid XYY call
                            hl.Call(alleles=[0, 0], phased=False),  # valid XXX call
                        ],
                    ],
                },
            )
            .key_rows_by('locus')
            .key_cols_by('s')
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            "Found samples with misaligned ploidy with their provided imputed sex \\(first 10, if applicable\\) : \\['HG00731_1', 'HG00732_1', 'NA20889_1', 'NA20899_1'\\].*",
            validate_imputed_sex_ploidy,
            mt,
            sex_check_ht,
        )

    def test_validate_imported_field_types(self) -> None:
        mt = hl.read_matrix_table(TEST_MITO_MT)
        validate_imported_field_types(mt, DatasetType.MITO, {})
        mt = mt.annotate_cols(contamination=hl.int32(mt.contamination))
        mt = mt.annotate_entries(DP=hl.float32(mt.DP))
        mt = mt.annotate_rows(vep=hl.dict({'t': '1'}))
        self.assertRaisesRegex(
            SeqrValidationError,
            "Found unexpected field types on MatrixTable after import: \\['contamination: int32', 'DP: float32', 'vep: dict<str, str>', 'tester: missing'\\]",
            validate_imported_field_types,
            mt,
            DatasetType.MITO,
            {'tester': hl.tfloat32},
        )

    def test_validate_no_duplicate_variants(self) -> None:
        mt = (
            hl.MatrixTable.from_parts(
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
                    'alleles': [
                        ['A', 'C'],
                        ['A', 'C'],
                        ['A', 'C'],
                    ],
                },
                cols={'s': ['sample_1']},
                entries={'HL': [[0.0], [0.0], [0.0]]},
            )
            .key_rows_by('locus', 'alleles')
            .key_cols_by('s')
        )
        self.assertRaisesRegex(
            SeqrValidationError,
            "Variants are present multiple times in the callset: \\['1-2-A-C'\\]",
            validate_no_duplicate_variants,
            mt,
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
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
