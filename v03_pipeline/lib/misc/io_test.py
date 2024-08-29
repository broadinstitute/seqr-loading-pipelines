import unittest

import hail as hl

from v03_pipeline.lib.misc.io import (
    compute_hail_n_partitions,
    file_size_bytes,
    import_imputed_sex,
    import_vcf,
    remap_pedigree_hash,
)
from v03_pipeline.lib.misc.validation import SeqrValidationError
from v03_pipeline.lib.model import ReferenceGenome

TEST_IMPUTED_SEX = 'v03_pipeline/var/test/sex_check/test_imputed_sex.tsv'
TEST_IMPUTED_SEX_UNEXPECTED_VALUE = (
    'v03_pipeline/var/test/sex_check/test_imputed_sex_unexpected_value.tsv'
)
TEST_PEDIGREE_3 = 'v03_pipeline/var/test/pedigrees/test_pedigree_3.tsv'
TEST_MITO_MT = 'v03_pipeline/var/test/callsets/mito_1.mt'
TEST_REMAP = 'v03_pipeline/var/test/remaps/test_remap_1.tsv'
TEST_SV_VCF = 'v03_pipeline/var/test/callsets/sv_1.vcf'


class IOTest(unittest.TestCase):
    def test_file_size_mb(self) -> None:
        # find v03_pipeline/var/test/callsets/mito_1.mt -type f | grep -v 'crc' | xargs ls -alt {} | awk '{sum += $5; print sum}'
        # 191310
        self.assertEqual(file_size_bytes(TEST_MITO_MT), 191310)
        self.assertEqual(file_size_bytes(TEST_SV_VCF), 20040)

    def test_compute_hail_n_partitions(self) -> None:
        self.assertEqual(compute_hail_n_partitions(23), 1)
        self.assertEqual(compute_hail_n_partitions(191310), 1)
        self.assertEqual(compute_hail_n_partitions(1913100000), 15)

    def test_import_imputed_sex(self) -> None:
        ht = import_imputed_sex(TEST_IMPUTED_SEX)
        self.assertListEqual(
            ht.collect(),
            [
                hl.Struct(s='abc_1', predicted_sex='M'),
                hl.Struct(s='abc_2', predicted_sex='F'),
                hl.Struct(s='abc_3', predicted_sex='M'),
            ],
        )

    def test_import_imputed_sex_unexpected_value(self) -> None:
        ht = import_imputed_sex(TEST_IMPUTED_SEX_UNEXPECTED_VALUE)
        self.assertRaisesRegex(
            hl.utils.java.HailUserError,
            'Found unexpected value Unknown in imputed sex file',
            ht.collect,
        )

    def test_remap_pedigree_hash(self) -> None:
        self.assertEqual(
            hl.eval(
                remap_pedigree_hash(
                    TEST_REMAP,
                    TEST_PEDIGREE_3,
                ),
            ),
            -560434714,
        )

    def test_import_bad_callset(self) -> None:
        self.assertRaisesRegex(
            SeqrValidationError,
            '.*failed initial file format(?s).*We never saw the required CHROM header line.*',
            import_vcf,
            TEST_PEDIGREE_3,
            ReferenceGenome.GRCh38,
        )
