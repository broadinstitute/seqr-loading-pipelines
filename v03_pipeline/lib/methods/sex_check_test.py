import unittest

import hail as hl

from v03_pipeline.lib.methods.sex_check import (
    call_sex,
    generate_fstat_plot,
    get_contig_cov,
)
from v03_pipeline.lib.model import ReferenceGenome

TEST_FSTAT_PLOT = 'v03_pipeline/var/test/plots/f_stat_plot_1.png'
TEST_SEX_AND_RELATEDNESS_CALLSET_MT = (
    'v03_pipeline/var/test/callsets/sex_and_relatedness_1.mt'
)


class SexCheckTest(unittest.TestCase):
    def test_invalid_contig(self):
        self.assertRaises(
            ValueError,
            get_contig_cov,
            None,
            ReferenceGenome.GRCh38,
            '1',
            0.25,
            'AF',
        )

    def test_get_contig_cov(self):
        mt = hl.MatrixTable.from_parts(
            rows={
                'locus': [
                    hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr1',
                        position=3,
                        reference_genome='GRCh38',
                    ),
                    hl.Locus(
                        contig='chr2',
                        position=4,
                        reference_genome='GRCh38',
                    ),
                ],
                'alleles': [
                    ['A', 'C'],
                    ['A', 'C'],
                    ['A', 'C'],
                    ['A', 'C'],
                ],
                'AF': [0.1, 0.02, 0.001, 0.1],
            },
            cols={'s': ['sample_1', 'sample_2']},
            entries={
                'DP': [
                    [0.0, hl.missing(hl.tfloat)],
                    [0.1, 0.3],
                    [hl.missing(hl.tfloat), 0.5],
                    [0.4, 0.6],
                ],
                'GT': [
                    [
                        hl.Call(alleles=[0, 0], phased=False),
                        hl.Call(alleles=[0, 0], phased=False),
                    ],
                    [
                        hl.Call(alleles=[0, 0], phased=False),
                        hl.Call(alleles=[0, 0], phased=False),
                    ],
                    [
                        hl.Call(alleles=[0, 0], phased=False),
                        hl.Call(alleles=[0, 0], phased=False),
                    ],
                    [
                        hl.Call(alleles=[0, 0], phased=False),
                        hl.Call(alleles=[0, 0], phased=False),
                    ],
                ],
            },
        )
        mt = mt.key_rows_by('locus', 'alleles')
        mt = mt.key_cols_by('s')
        ht = get_contig_cov(mt, ReferenceGenome.GRCh38, 'chr1', 0.25, 'AF')
        self.assertListEqual(
            ht.collect(),
            [
                hl.Struct(s='sample_1', chr1_mean_dp=0.05),
                hl.Struct(s='sample_2', chr1_mean_dp=0.3),
            ],
        )

    def test_call_sex(self):
        mt = hl.read_matrix_table(TEST_SEX_AND_RELATEDNESS_CALLSET_MT)
        ht = call_sex(mt, ReferenceGenome.GRCh38, af_field='AF')
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
        f_stat_plot = generate_fstat_plot(ht, 0.75, 0.5)
        with open(TEST_FSTAT_PLOT, 'rb') as f:
            self.assertEqual(
                f_stat_plot.read(),
                f.read(),
            )

    def test_call_sex_w_chrY_coverage(self):  # noqa: N802
        mt = hl.read_matrix_table(TEST_SEX_AND_RELATEDNESS_CALLSET_MT)
        ht = call_sex(mt, ReferenceGenome.GRCh38, use_chrY_cov=True, af_field='AF')
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
                    sex='sex_aneuploidy',
                    chr20_mean_dp=36.75,
                    chrY_mean_dp=1.0,
                    normalized_y_coverage=0.027210884353741496,
                    chrX_mean_dp=32.91891891891892,
                    normalized_x_coverage=0.8957528957528957,
                ),
                hl.Struct(
                    s='ROS_006_18Y03227_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                    chr20_mean_dp=40.75,
                    chrY_mean_dp=16.5,
                    normalized_y_coverage=0.4049079754601227,
                    chrX_mean_dp=18.945945945945947,
                    normalized_x_coverage=0.4649311888575693,
                ),
                hl.Struct(
                    s='ROS_006_18Y03228_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                    chr20_mean_dp=35.5,
                    chrY_mean_dp=17.375,
                    normalized_y_coverage=0.4894366197183099,
                    chrX_mean_dp=17.08108108108108,
                    normalized_x_coverage=0.48115721355157975,
                ),
                hl.Struct(
                    s='ROS_007_19Y05919_D1',
                    is_female=False,
                    f_stat=0.9016393442622951,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=26,
                    sex='XY',
                    chr20_mean_dp=46.5,
                    chrY_mean_dp=23.75,
                    normalized_y_coverage=0.510752688172043,
                    chrX_mean_dp=21.243243243243242,
                    normalized_x_coverage=0.4568439407149084,
                ),
                hl.Struct(
                    s='ROS_007_19Y05939_D1',
                    is_female=True,
                    f_stat=-0.08196721311475397,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=16,
                    sex='XX',
                    chr20_mean_dp=35.75,
                    chrY_mean_dp=1.0,
                    normalized_y_coverage=0.027972027972027972,
                    chrX_mean_dp=39.32432432432432,
                    normalized_x_coverage=1.0999810999811,
                ),
                hl.Struct(
                    s='ROS_007_19Y05987_D1',
                    is_female=False,
                    f_stat=1.0,
                    n_called=27,
                    expected_homs=16.833333333333332,
                    observed_homs=27,
                    sex='XY',
                    chr20_mean_dp=31.75,
                    chrY_mean_dp=16.875,
                    normalized_y_coverage=0.531496062992126,
                    chrX_mean_dp=17.0,
                    normalized_x_coverage=0.5354330708661418,
                ),
            ],
        )
