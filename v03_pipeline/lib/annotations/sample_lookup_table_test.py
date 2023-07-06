import unittest

import hail as hl

from v03_pipeline.lib.annotations.sample_lookup_table import gt_stats


class SampleLookupTableAnnotationsTest(unittest.TestCase):
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
                    'ref_samples': {'project_1': {'a', 'c'}},
                    'het_samples': {'project_1': {'b', 'd'}},
                    'hom_samples': {'project_1': {'e', 'f'}},
                },
                {
                    'id': 1,
                    'ref_samples': {
                        'project_1': {'a', 'b', 'c', 'd', 'e', 'f'},
                        'project_2': {'x'},
                    },
                    'het_samples': {'project_1': set(), 'project_2': {'y'}},
                    'hom_samples': {'project_1': set(), 'project_2': {'z'}},
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tdict(hl.tstr, hl.tset(hl.tstr)),
                het_samples=hl.tdict(hl.tstr, hl.tset(hl.tstr)),
                hom_samples=hl.tdict(hl.tstr, hl.tset(hl.tstr)),
            ),
            key='id',
            globals=hl.Struct(
                updates=hl.set([hl.Struct(callset='abc', project_guid='project_1')]),
            ),
        )
        ht = ht.select(gt_stats=gt_stats(ht, sample_lookup_ht))
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(id=0, gt_stats=hl.Struct(AC=6, AF=0.5, AN=12, hom=2)),
                hl.Struct(
                    id=1,
                    gt_stats=hl.Struct(AC=3, AN=18, AF=0.1666666716337204, hom=1),
                ),
            ],
        )
