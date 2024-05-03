import unittest

from v03_pipeline.lib.misc.io import (
    compute_hail_n_partitions,
    file_size_bytes,
)

TEST_MITO_MT = 'v03_pipeline/var/test/callsets/mito_1.mt'
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
