import unittest
from unittest.mock import patch

import hail as hl

from v03_pipeline.lib.methods.sex_check import impute_sex

TEST_SEX_AND_RELATEDNESS_CALLSET_MT = (
    'v03_pipeline/var/test/callsets/sex_and_relatedness_1.mt'
)
TEST_PEDIGREE = 'v03_pipeline/var/test/pedigrees/test_pedigree_6.tsv'


class SexCheckTest(unittest.TestCase):
    def test_impute_sex(self):
        mt = hl.read_matrix_table(TEST_SEX_AND_RELATEDNESS_CALLSET_MT)
        ht = impute_sex(mt)
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    s='ROS_006_18Y03226_D1',
                    predicted_sex='M',
                ),
                hl.Struct(
                    s='ROS_006_18Y03227_D1',
                    predicted_sex='M',
                ),
                hl.Struct(
                    s='ROS_006_18Y03228_D1',
                    predicted_sex='M',
                ),
                hl.Struct(
                    s='ROS_007_19Y05919_D1',
                    predicted_sex='M',
                ),
                hl.Struct(
                    s='ROS_007_19Y05939_D1',
                    predicted_sex='F',
                ),
                hl.Struct(
                    s='ROS_007_19Y05987_D1',
                    predicted_sex='M',
                ),
            ],
        )

    def test_impute_sex_ambiguous(self):
        mt = hl.read_matrix_table(TEST_SEX_AND_RELATEDNESS_CALLSET_MT)
        with patch('v03_pipeline.lib.methods.sex_check.XY_FSTAT_THRESHOLD', 0.95):
            self.assertRaises(
                ValueError,
                impute_sex,
                mt,
            )
