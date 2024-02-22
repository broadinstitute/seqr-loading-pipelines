import unittest

import hail as hl

from v03_pipeline.lib.annotations.snv_indel import gt_stats


class SNVTest(unittest.TestCase):
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
        lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': hl.Struct(
                        project_1={'a', 'c'},
                        project_2=set(),
                        R0607_gregor_training_project_=set(),
                    ),
                    'het_samples': hl.Struct(
                        project_1={'b', 'd'},
                        project_2=set(),
                        R0607_gregor_training_project_=set(),
                    ),
                    'hom_samples': hl.Struct(
                        project_1={'e', 'f'},
                        project_2=set(),
                        R0607_gregor_training_project_={'l', 'm'},
                    ),
                },
                {
                    'id': 1,
                    'ref_samples': hl.Struct(
                        project_1={'a', 'b', 'c', 'd', 'e', 'f'},
                        project_2=set(),
                        R0607_gregor_training_project_={'l', 'm'},
                    ),
                    'het_samples': hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        R0607_gregor_training_project_=set(),
                    ),
                    'hom_samples': hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        R0607_gregor_training_project_=set(),
                    ),
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tstruct(
                    project_1=hl.tset(hl.tstr),
                    project_2=hl.tset(hl.tstr),
                    R0607_gregor_training_project_=hl.tset(hl.tstr),
                ),
                het_samples=hl.tstruct(
                    project_1=hl.tset(hl.tstr),
                    project_2=hl.tset(hl.tstr),
                    R0607_gregor_training_project_=hl.tset(hl.tstr),
                ),
                hom_samples=hl.tstruct(
                    project_1=hl.tset(hl.tstr),
                    project_2=hl.tset(hl.tstr),
                    R0607_gregor_training_project_=hl.tset(hl.tstr),
                ),
            ),
            key='id',
        )
        ht = ht.select(gt_stats=gt_stats(ht, lookup_ht))
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(id=0, gt_stats=hl.Struct(AC=6, AF=0.5, AN=12, hom=2)),
                hl.Struct(
                    id=1,
                    gt_stats=hl.Struct(AC=0, AN=12, AF=0.0, hom=0),
                ),
            ],
        )
