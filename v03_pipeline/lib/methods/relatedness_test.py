import unittest

import hail as hl

from v03_pipeline.lib.methods.relatedness import (
    build_relatedness_check_lookup,
    call_relatedness,
)
from v03_pipeline.lib.model import ReferenceGenome

TEST_SEX_AND_RELATEDNESS_CALLSET_MT = (
    'v03_pipeline/var/test/callsets/sex_and_relatedness_1.mt'
)


class RelatednessTest(unittest.TestCase):
    def test_call_relatedness(self):
        mt = hl.read_matrix_table(TEST_SEX_AND_RELATEDNESS_CALLSET_MT)
        ht = call_relatedness(
            mt,
            ReferenceGenome.GRCh38,
            af_field='AF',
            use_gnomad_in_ld_prune=False,
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    i='ROS_006_18Y03226_D1',
                    j='ROS_007_19Y05939_D1',
                    ibd0=0.0,
                    ibd1=1.0,
                    ibd2=0.0,
                    pi_hat=0.5,
                ),
            ],
        )
        self.assertEqual(
            build_relatedness_check_lookup(
                ht,
                hl.dict({'ROS_006_18Y03226_D1': 'remapped_id'}),
            ),
            {
                ('remapped_id', 'ROS_007_19Y05939_D1'): hl.Struct(
                    ibd0=0.0,
                    ibd1=1.0,
                    ibd2=0.0,
                    pi_hat=0.5,
                ),
            },
        )
