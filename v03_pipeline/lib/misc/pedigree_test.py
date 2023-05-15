import unittest

import hail as hl

from v03_pipeline.lib.misc.pedigree import (
    families_to_exclude,
    families_to_include,
    import_pedigree,
    samples_to_include,
)

TEST_PEDIGREE_1 = 'v03_pipeline/var/test/test_pedigree_1.tsv'
TEST_PEDIGREE_2 = 'v03_pipeline/var/test/test_pedigree_2.tsv'


class DownloadUtilsTest(unittest.TestCase):
    def test_empty_pedigree(self) -> None:
        with self.assertRaises(ValueError):
            _ = import_pedigree(TEST_PEDIGREE_1)

    def test_parse_project(self) -> None:
        pedigree_ht = import_pedigree(TEST_PEDIGREE_2)
        samples_ht = hl.Table.parallelize(
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

        self.assertCountEqual(
            [
                f.family_id
                for f in families_to_exclude(pedigree_ht, samples_ht).collect()
            ],
            ['BBL_HT-007-5195'],
        )
        self.assertCountEqual(
            [
                f.family_id
                for f in families_to_include(pedigree_ht, samples_ht).collect()
            ],
            [
                'BBL_SDS1-000178',
                'BBL_BC1-000345',
            ],
        )
        self.assertCountEqual(
            [s.s for s in samples_to_include(pedigree_ht, samples_ht).collect()],
            [
                'BBL_SDS1-000178_01_D1',
                'BBL_BC1-000345_01_D1',
                'BBL_BC1-000345_02_D1',
                'BBL_BC1-000345_03_D1',
            ],
        )
