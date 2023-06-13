import unittest

import hail as hl

from v03_pipeline.lib.misc.sample_lookup import (
    remove_callset_sample_ids,
    union_sample_lookup_hts,
)


class SampleLookupTest(unittest.TestCase):
    def test_remove_callset_sample_ids(self) -> None:
        sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': hl.Struct(project_1=set()),
                    'het_samples': hl.Struct(project_1={'b', 'd', 'f'}),
                    'hom_samples': hl.Struct(project_1={'e', 'f'}),
                },
                {
                    'id': 1,
                    'ref_samples': hl.Struct(project_1={'f'}),
                    'het_samples': hl.Struct(project_1={'a'}),
                    'hom_samples': hl.Struct(project_1=set()),
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
                het_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
                hom_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
            ),
            key='id',
        )
        samples_ht = hl.Table.parallelize(
            [
                {'s': 'd'},
                {'s': 'e'},
                {'s': 'f'},
            ],
            hl.tstruct(
                s=hl.dtype('str'),
            ),
            key='s',
        )
        sample_lookup_ht = remove_callset_sample_ids(
            sample_lookup_ht,
            samples_ht,
            'project_1',
        )
        self.assertListEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples=hl.Struct(project_1=set()),
                    het_samples=hl.Struct(project_1={'b'}),
                    hom_samples=hl.Struct(project_1=set()),
                ),
                hl.Struct(
                    id=1,
                    ref_samples=hl.Struct(project_1=set()),
                    het_samples=hl.Struct(project_1={'a'}),
                    hom_samples=hl.Struct(project_1=set()),
                ),
            ],
        )

    def test_union_sample_lookup_hts(self) -> None:
        sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': hl.Struct(project_1=set()),
                    'het_samples': hl.Struct(project_1={'b', 'd', 'f'}),
                    'hom_samples': hl.Struct(project_1={'e', 'f'}),
                },
                {
                    'id': 1,
                    'ref_samples': hl.Struct(project_1={'f'}),
                    'het_samples': hl.Struct(project_1={'a'}),
                    'hom_samples': hl.Struct(project_1=set()),
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
                het_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
                hom_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
            ),
            key='id',
        )
        callset_sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': hl.Struct(project_1={'e'}),
                    'het_samples': hl.Struct(project_1={'f', 'g'}),
                    'hom_samples': hl.Struct(project_1=set()),
                },
                {
                    'id': 2,
                    'ref_samples': hl.Struct(project_1={'c'}),
                    'het_samples': hl.Struct(project_1={'d'}),
                    'hom_samples': hl.Struct(project_1=set()),
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
                het_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
                hom_samples=hl.tstruct(project_1=hl.tset(hl.tstr)),
            ),
            key='id',
        )
        sample_lookup_ht = union_sample_lookup_hts(
            sample_lookup_ht,
            callset_sample_lookup_ht,
            'project_1',
        )
        self.assertCountEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples=hl.Struct(project_1={'e'}),
                    het_samples=hl.Struct(project_1={'b', 'd', 'f', 'g'}),
                    hom_samples=hl.Struct(project_1={'e', 'f'}),
                ),
                hl.Struct(
                    id=1,
                    ref_samples=hl.Struct(project_1={'f'}),
                    het_samples=hl.Struct(project_1={'a'}),
                    hom_samples=hl.Struct(project_1=set()),
                ),
                hl.Struct(
                    id=2,
                    ref_samples=hl.Struct(project_1={'c'}),
                    het_samples=hl.Struct(project_1={'d'}),
                    hom_samples=hl.Struct(project_1=set()),
                ),
            ],
        )
