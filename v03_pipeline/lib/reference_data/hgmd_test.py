import unittest

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_data.hgmd import download_and_import_hgmd_vcf

TEST_HGMD_VCF = 'v03_pipeline/var/test/reference_data/test_hgmd.vcf'


class HGMDTest(unittest.TestCase):
    def test_import_hgmd_vcf(self):
        ht = download_and_import_hgmd_vcf(TEST_HGMD_VCF, ReferenceGenome.GRCh38)
        self.assertEqual(ht.count(), 1)
