import unittest

import hail as hl

from v03_pipeline.lib.misc.io import import_pedigree, 
from v03_pipeline.lib.misc.pedigree import parse_pedigree_ht

TEST_PEDIGREE_1 = 'v03_pipeline/var/test/pedigrees/test_pedigree_1.tsv'
TEST_PEDIGREE_2 = 'v03_pipeline/var/test/pedigrees/test_pedigree_2.tsv'


class PedigreesTest(unittest.TestCase):
    def test_empty_pedigree(self) -> None:
        with self.assertRaises(ValueError):
            _ = import_pedigree(TEST_PEDIGREE_1)

    def test_parse_project(self) -> None:
        pedigree_ht = import_pedigree(TEST_PEDIGREE_2)
        families = parse_pedigree_ht(pedigree_ht)
        self.assertEqual(
            families,
            [],
        )
        hl.Table.parallelize(
            [
                {'s': 'BBL_SDS1-000178_01_D1'},
                {'s': 'BBL_HT-007-5195_01_D1'},
                {'s': 'BBL_HT-007-5195_02_D1'},
                {'s': 'BBL_HT-007-5195_03_D1'},
                {'s': 'BBL_HT-007-5195_04_D1'},
                # missing BBL_HT-007-5195_05_D1
                {'s': 'BBL_HT-007-5195_06_D1'},
                {'s': 'BBL_BC1-000345_01_D1'},
                {'s': 'BBL_BC1-000345_02_D1'},
                {'s': 'BBL_BC1-000345_03_D1'},
            ],
            hl.tstruct(
                s=hl.dtype('str'),
            ),
            key='s',
        )
