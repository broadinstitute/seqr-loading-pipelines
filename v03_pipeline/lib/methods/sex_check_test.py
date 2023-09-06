import unittest

import hail as hl

from v03_pipeline.lib.methods.sex_check import get_contig_cov
from v03_pipeline.lib.model import ReferenceGenome


class SexCheckTest(unittest.TestCase):
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
        ht = get_contig_cov(mt, ReferenceGenome.GRCh38, 'chr1', 0.25)
        self.assertListEqual(
            ht.collect(),
            [
                hl.Struct(s='sample_1', chr1_mean_dp=0.05),
                hl.Struct(s='sample_2', chr1_mean_dp=0.3),
            ],
        )
