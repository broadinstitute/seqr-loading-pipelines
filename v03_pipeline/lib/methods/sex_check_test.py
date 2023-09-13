import unittest
from unittest.mock import patch

import hail as hl

from v03_pipeline.lib.methods.sex_check import annotate_discrepant_sex, call_sex
from v03_pipeline.lib.misc.io import import_pedigree

TEST_SEX_AND_RELATEDNESS_CALLSET_MT = (
    'v03_pipeline/var/test/callsets/sex_and_relatedness_1.mt'
)
TEST_PEDIGREE = 'v03_pipeline/var/test/pedigrees/test_pedigree_6.tsv'


class SexCheckTest(unittest.TestCase):
    def test_call_sex(self):
        mt = hl.read_matrix_table(TEST_SEX_AND_RELATEDNESS_CALLSET_MT)
        ht = call_sex(mt)
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    s='ROS_006_18Y03226_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                ),
                hl.Struct(
                    s='ROS_006_18Y03227_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                ),
                hl.Struct(
                    s='ROS_006_18Y03228_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                ),
                hl.Struct(
                    s='ROS_007_19Y05919_D1',
                    is_female=False,
                    f_stat=0.9016393442622951,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=26,
                    sex='XY',
                ),
                hl.Struct(
                    s='ROS_007_19Y05939_D1',
                    is_female=True,
                    f_stat=-0.08196721311475397,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=16,
                    sex='XX',
                ),
                hl.Struct(
                    s='ROS_007_19Y05987_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                ),
            ],
        )
        pedigree_ht = import_pedigree(TEST_PEDIGREE)
        ht = annotate_discrepant_sex(ht, pedigree_ht)
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    s='ROS_006_18Y03226_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                    given_sex='XX',
                    discrepant_sex=True,
                ),
                hl.Struct(
                    s='ROS_006_18Y03227_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                    given_sex='XY',
                    discrepant_sex=False,
                ),
                hl.Struct(
                    s='ROS_006_18Y03228_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                    given_sex='XX',
                    discrepant_sex=True,
                ),
                hl.Struct(
                    s='ROS_007_19Y05919_D1',
                    is_female=False,
                    f_stat=0.9016393442622951,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=26,
                    sex='XY',
                    given_sex='XX',
                    discrepant_sex=True,
                ),
                hl.Struct(
                    s='ROS_007_19Y05939_D1',
                    is_female=True,
                    f_stat=-0.08196721311475397,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=16,
                    sex='XX',
                    given_sex='XX',
                    discrepant_sex=False,
                ),
                hl.Struct(
                    s='ROS_007_19Y05987_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                    given_sex='XX',
                    discrepant_sex=True,
                ),
            ],
        )

    def test_call_sex_ambiguous(self):
        mt = hl.read_matrix_table(TEST_SEX_AND_RELATEDNESS_CALLSET_MT)
        with patch('v03_pipeline.lib.methods.sex_check.XY_FSTAT_THRESHOLD', 0.95):
            self.assertRaises(
                ValueError,
                call_sex,
                mt,
            )
