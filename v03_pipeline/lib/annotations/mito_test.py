import unittest

import hail as hl

from v03_pipeline.lib.annotations.mito import gt_stats


class MITOTest(unittest.TestCase):
    def test_allele_count_annotations(self) -> None:
        ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                },
                {
                    'id': 1,
                },
            ],
            hl.tstruct(
                id=hl.tint32,
            ),
            key='id',
        )
        sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': hl.Struct(project_1={'a', 'c'}, project_2=set()),
                    'heteroplasmic_samples': hl.Struct(
                        project_1={'b', 'd'},
                        project_2=set(),
                    ),
                    'homoplasmic_samples': hl.Struct(
                        project_1={'e', 'f'},
                        project_2=set(),
                    ),
                },
                {
                    'id': 1,
                    'ref_samples': hl.Struct(
                        project_1={'a', 'b', 'c', 'd', 'e', 'f'},
                        project_2=set(),
                    ),
                    'heteroplasmic_samples': hl.Struct(
                        project_1=set(),
                        project_2=set(),
                    ),
                    'homoplasmic_samples': hl.Struct(project_1=set(), project_2=set()),
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tstruct(
                    project_1=hl.tset(hl.tstr),
                    project_2=hl.tset(hl.tstr),
                ),
                heteroplasmic_samples=hl.tstruct(
                    project_1=hl.tset(hl.tstr),
                    project_2=hl.tset(hl.tstr),
                ),
                homoplasmic_samples=hl.tstruct(
                    project_1=hl.tset(hl.tstr),
                    project_2=hl.tset(hl.tstr),
                ),
            ),
            key='id',
        )
        ht = ht.select(gt_stats=gt_stats(ht, sample_lookup_ht))
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    gt_stats=hl.Struct(
                        AC_het=2,
                        AF_het=0.3333333432674408,
                        AC_hom=2,
                        AF_hom=0.3333333432674408,
                        AN=6,
                    ),
                ),
                hl.Struct(
                    id=1,
                    gt_stats=hl.Struct(
                        AC_het=0,
                        AF_het=0.0,
                        AC_hom=0,
                        AF_hom=0.0,
                        AN=6,
                    ),
                ),
            ],
        )
