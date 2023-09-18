import unittest

import hail as hl

from v03_pipeline.lib.misc.family_loading_failures import build_sex_check_lookup
from v03_pipeline.lib.model import Ploidy


class FamilyLoadingFailuresTest(unittest.TestCase):
    def test_build_sex_check_lookup(self):
        ht = hl.Table.parallelize(
            [
                {'s': 'remapped_id', 'sex': 'M'},
                {'s': 'ROS_006_18Y03227_D1', 'sex': 'M'},
                {'s': 'ROS_006_18Y03228_D1', 'sex': 'M'},
                {'s': 'ROS_007_19Y05919_D1', 'sex': 'M'},
                {'s': 'ROS_007_19Y05939_D1', 'sex': 'F'},
                {'s': 'ROS_007_19Y05987_D1', 'sex': 'M'},
            ],
            hl.tstruct(
                s=hl.tstr,
                sex=hl.tstr,
            ),
            key='s',
        )
        self.assertEqual(
            build_sex_check_lookup(ht, hl.dict({'ROS_006_18Y03226_D1': 'remapped_id'})),
            {
                'remapped_id': Ploidy.MALE,
                'ROS_006_18Y03227_D1': Ploidy.MALE,
                'ROS_006_18Y03228_D1': Ploidy.MALE,
                'ROS_007_19Y05919_D1': Ploidy.MALE,
                'ROS_007_19Y05939_D1': Ploidy.FEMALE,
                'ROS_007_19Y05987_D1': Ploidy.MALE,
            },
        )
