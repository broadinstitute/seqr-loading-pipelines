import unittest

import hail as hl

from v03_pipeline.lib.misc.family_loading_failures import (
    build_relatedness_check_lookup,
    build_sex_check_lookup,
    passes_all_relatedness_checks,
)
from v03_pipeline.lib.misc.pedigree import Sample
from v03_pipeline.lib.model import Ploidy


class FamilyLoadingFailuresTest(unittest.TestCase):
    def test_build_relatedness_check_lookup(self):
        ht = hl.Table.parallelize(
            [
                {
                    'i': 'ROS_006_18Y03226_D1',
                    'j': 'ROS_007_19Y05939_D1',
                    'ibd0': 0.0,
                    'ibd1': 1.0,
                    'ibd2': 0.0,
                    'pi_hat': 0.5,
                },
            ],
            hl.tstruct(
                i=hl.tstr,
                j=hl.tstr,
                ibd0=hl.tfloat,
                ibd1=hl.tfloat,
                ibd2=hl.tfloat,
                pi_hat=hl.tfloat,
            ),
            key=['i', 'j'],
        )
        self.assertEqual(
            build_relatedness_check_lookup(
                ht,
                hl.dict({'ROS_006_18Y03226_D1': 'remapped_id'}),
            ),
            {
                ('remapped_id', 'ROS_007_19Y05939_D1'): [
                    0.0,
                    1.0,
                    0.0,
                    0.5,
                ],
            },
        )

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

    def test_passes_all_relatedness_checks(self):
        relatedness_check_lookup = {
            # Parent
            ('sample_1', 'sample_2'): [
                0.0,
                0.98,
                0.0,
                0.52,
            ],
            # GrandParent
            ('sample_1', 'sample_3'): [0.47, 0.53, 0, 0.23],
            # Half Sibling (but actually a hidden Sibling)
            ('sample_1', 'sample_4'): [0.25, 0.5, 0.25, 0.5],
        }
        sample = Sample(
            sex=Ploidy.FEMALE,
            sample_id='sample_1',
            mother='sample_2',
            paternal_grandfather='sample_3',
            half_siblings=['sample_4'],
        )
        self.assertTrue(
            passes_all_relatedness_checks(relatedness_check_lookup, sample),
        )

        # Sibling is actually a half sibling.
        relatedness_check_lookup = {
            **relatedness_check_lookup,
            ('sample_1', 'sample_4'): [0.5, 0.5, 0, 0.25],
        }
        sample = Sample(
            sex=Ploidy.FEMALE,
            sample_id='sample_1',
            mother='sample_2',
            paternal_grandfather='sample_3',
            siblings=['sample_4'],
        )
        self.assertFalse(
            passes_all_relatedness_checks(relatedness_check_lookup, sample),
        )
