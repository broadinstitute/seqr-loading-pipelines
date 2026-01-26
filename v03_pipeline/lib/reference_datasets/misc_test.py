import unittest

from v03_pipeline.lib.core.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.misc import vcf_to_ht

EXAC_PATH = 'v03_pipeline/var/test/reference_datasets/raw/exac.vcf'


class MiscTest(unittest.TestCase):
    def test_vcf_to_ht_throw_multiallelic(self):
        self.assertRaises(
            ValueError,
            vcf_to_ht,
            EXAC_PATH,
            ReferenceGenome.GRCh38,
        )
