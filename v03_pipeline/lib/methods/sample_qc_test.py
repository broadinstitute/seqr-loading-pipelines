import unittest

import hail as hl

from v03_pipeline.lib.methods.sample_qc import call_sample_qc

TEST_CALLSET_MT = 'v03_pipeline/var/test/callsets/sex_and_relatedness_1.mt'


class SampleQCTest(unittest.TestCase):
    def test_call_sample_qc(self):
        mt = hl.read_matrix_table(TEST_CALLSET_MT)
        mt = call_sample_qc(mt)
        self.assertCountEqual(
            mt.cols().collect(),
            [
                hl.Struct(
                    s='ROS_006_18Y03226_D1',
                    seqr_id='ROS_006_18Y03226_D1',
                    vcf_id='ROS_006_18Y03226_D1',
                    filtered_callrate=1.0,
                ),
                hl.Struct(
                    s='ROS_006_18Y03227_D1',
                    seqr_id='ROS_006_18Y03227_D1',
                    vcf_id='ROS_006_18Y03227_D1',
                    filtered_callrate=1.0,
                ),
                hl.Struct(
                    s='ROS_006_18Y03228_D1',
                    seqr_id='ROS_006_18Y03228_D1',
                    vcf_id='ROS_006_18Y03228_D1',
                    filtered_callrate=1.0,
                ),
                hl.Struct(
                    s='ROS_007_19Y05919_D1',
                    seqr_id='ROS_007_19Y05919_D1',
                    vcf_id='ROS_007_19Y05919_D1',
                    filtered_callrate=1.0,
                ),
                hl.Struct(
                    s='ROS_007_19Y05939_D1',
                    seqr_id='ROS_007_19Y05939_D1',
                    vcf_id='ROS_007_19Y05939_D1',
                    filtered_callrate=1.0,
                ),
                hl.Struct(
                    s='ROS_007_19Y05987_D1',
                    seqr_id='ROS_007_19Y05987_D1',
                    vcf_id='ROS_007_19Y05987_D1',
                    filtered_callrate=1.0,
                ),
            ],
        )
