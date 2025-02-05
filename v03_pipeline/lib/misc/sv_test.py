import unittest

from v03_pipeline.lib.misc.sv import overwrite_male_non_par_calls
from v03_pipeline.lib.pedigree import Family, Sample

TEST_SV_VCF = 'v03_pipeline/var/test/callsets/sv_1.vcf'


class SVTest(unittest.TestCase):
    def test_overwrite_male_non_par_calls(self) -> None:
        pass
