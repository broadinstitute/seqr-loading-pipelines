import unittest

import hail as hl

from v03_pipeline.lib.misc.sample_lookup import (
    compute_callset_sample_lookup_ht,
    filter_callset_sample_ids,
    join_sample_lookup_hts,
)
from v03_pipeline.lib.model import DatasetType


class SampleLookupTest(unittest.TestCase):
    def test_compute_callset_sample_lookup_ht(self) -> None:
        mt = hl.MatrixTable.from_parts(
            rows={'variants': [1, 2]},
            cols={'s': ['sample_1', 'sample_2', 'sample_3', 'sample_4']},
            entries={
                'HL': [
                    [0.0, hl.missing(hl.tfloat), 0.99, 0.01],
                    [0.1, 0.2, 0.94, 0.99],
                ],
            },
        )
        sample_lookup_ht = compute_callset_sample_lookup_ht(
            DatasetType.MITO,
            mt,
        )
        self.assertListEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    row_idx=0,
                    ref_samples={'sample_1'},
                    heteroplasmic_samples={'sample_4'},
                    homoplasmic_samples={'sample_3'},
                ),
                hl.Struct(
                    row_idx=1,
                    ref_samples=set(),
                    heteroplasmic_samples={'sample_1', 'sample_2', 'sample_3'},
                    homoplasmic_samples={'sample_4'},
                ),
            ],
        )

    def test_filter_callset_sample_ids(self) -> None:
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
            globals=hl.Struct(
                updates=hl.set([hl.Struct(callset='abc', project_guid='project_1')]),
            ),
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
        sample_lookup_ht = filter_callset_sample_ids(
            DatasetType.SNV_INDEL,
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
        samples_ht = hl.Table.parallelize(
            [
                {'s': 'b'},
            ],
            hl.tstruct(
                s=hl.dtype('str'),
            ),
            key='s',
        )
        sample_lookup_ht = filter_callset_sample_ids(
            DatasetType.SNV_INDEL,
            sample_lookup_ht,
            samples_ht,
            'project_2',
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

    def test_join_sample_lookup_hts(self) -> None:
        sample_lookup_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tstruct(),
                het_samples=hl.tstruct(),
                hom_samples=hl.tstruct(),
            ),
            key='id',
        )
        callset_sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': {'a'},
                    'het_samples': {'b', 'c'},
                    'hom_samples': set(),
                },
                {
                    'id': 1,
                    'ref_samples': set(),
                    'het_samples': set(),
                    'hom_samples': {'a', 'b', 'c'},
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tset(hl.tstr),
                het_samples=hl.tset(hl.tstr),
                hom_samples=hl.tset(hl.tstr),
            ),
            key='id',
        )
        sample_lookup_ht = join_sample_lookup_hts(
            DatasetType.SNV_INDEL,
            sample_lookup_ht,
            callset_sample_lookup_ht,
            'project_1',
        )
        self.assertCountEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples=hl.Struct(project_1={'a'}),
                    het_samples=hl.Struct(project_1={'b', 'c'}),
                    hom_samples=hl.Struct(project_1=set()),
                ),
                hl.Struct(
                    id=1,
                    ref_samples=hl.Struct(project_1=set()),
                    het_samples=hl.Struct(project_1=set()),
                    hom_samples=hl.Struct(project_1={'a', 'b', 'c'}),
                ),
            ],
        )
        callset_sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': {'a'},
                    'het_samples': {'b'},
                    'hom_samples': set(),
                },
                {
                    'id': 2,
                    'ref_samples': set(),
                    'het_samples': {'a'},
                    'hom_samples': {'b'},
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tset(hl.tstr),
                het_samples=hl.tset(hl.tstr),
                hom_samples=hl.tset(hl.tstr),
            ),
            key='id',
        )
        sample_lookup_ht = join_sample_lookup_hts(
            DatasetType.SNV_INDEL,
            sample_lookup_ht,
            callset_sample_lookup_ht,
            'project_2',
        )
        self.assertCountEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples=hl.Struct(
                        project_1={'a'},
                        project_2={'a'},
                    ),
                    het_samples=hl.Struct(
                        project_1={'b', 'c'},
                        project_2={'b'},
                    ),
                    hom_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                    ),
                ),
                hl.Struct(
                    id=1,
                    ref_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                    ),
                    het_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                    ),
                    hom_samples=hl.Struct(
                        project_1={'a', 'b', 'c'},
                        project_2=set(),
                    ),
                ),
                hl.Struct(
                    id=2,
                    ref_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                    ),
                    het_samples=hl.Struct(
                        project_1=set(),
                        project_2={'a'},
                    ),
                    hom_samples=hl.Struct(
                        project_1=set(),
                        project_2={'b'},
                    ),
                ),
            ],
        )
        callset_sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': {'c'},
                    'het_samples': {'d'},
                    'hom_samples': {'e'},
                },
                {
                    'id': 3,
                    'ref_samples': set(),
                    'het_samples': {'c'},
                    'hom_samples': {'d', 'e'},
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tset(hl.tstr),
                het_samples=hl.tset(hl.tstr),
                hom_samples=hl.tset(hl.tstr),
            ),
            key='id',
        )
        sample_lookup_ht = join_sample_lookup_hts(
            DatasetType.SNV_INDEL,
            sample_lookup_ht,
            callset_sample_lookup_ht,
            'project_3',
        )
        self.assertCountEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples=hl.Struct(
                        project_1={'a'},
                        project_2={'a'},
                        project_3={'c'},
                    ),
                    het_samples=hl.Struct(
                        project_1={'b', 'c'},
                        project_2={'b'},
                        project_3={'d'},
                    ),
                    hom_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3={'e'},
                    ),
                ),
                hl.Struct(
                    id=1,
                    ref_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3=set(),
                    ),
                    het_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3=set(),
                    ),
                    hom_samples=hl.Struct(
                        project_1={'a', 'b', 'c'},
                        project_2=set(),
                        project_3=set(),
                    ),
                ),
                hl.Struct(
                    id=2,
                    ref_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3=set(),
                    ),
                    het_samples=hl.Struct(
                        project_1=set(),
                        project_2={'a'},
                        project_3=set(),
                    ),
                    hom_samples=hl.Struct(
                        project_1=set(),
                        project_2={'b'},
                        project_3=set(),
                    ),
                ),
                hl.Struct(
                    id=3,
                    ref_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3=set(),
                    ),
                    het_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3={'c'},
                    ),
                    hom_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3={'d', 'e'},
                    ),
                ),
            ],
        )
        callset_sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': {'a', 'd'},
                    'het_samples': {'b', 'f'},
                    'hom_samples': {'c'},
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                ref_samples=hl.tset(hl.tstr),
                het_samples=hl.tset(hl.tstr),
                hom_samples=hl.tset(hl.tstr),
            ),
            key='id',
        )
        sample_lookup_ht = join_sample_lookup_hts(
            DatasetType.SNV_INDEL,
            sample_lookup_ht,
            callset_sample_lookup_ht,
            'project_2',
        )
        self.assertCountEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples=hl.Struct(
                        project_1={'a'},
                        project_2={'a', 'd'},
                        project_3={'c'},
                    ),
                    het_samples=hl.Struct(
                        project_1={'b', 'c'},
                        project_2={'b', 'f'},
                        project_3={'d'},
                    ),
                    hom_samples=hl.Struct(
                        project_1=set(),
                        project_2={'c'},
                        project_3={'e'},
                    ),
                ),
                hl.Struct(
                    id=1,
                    ref_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3=set(),
                    ),
                    het_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3=set(),
                    ),
                    hom_samples=hl.Struct(
                        project_1={'a', 'b', 'c'},
                        project_2=set(),
                        project_3=set(),
                    ),
                ),
                hl.Struct(
                    id=2,
                    ref_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3=set(),
                    ),
                    het_samples=hl.Struct(
                        project_1=set(),
                        project_2={'a'},
                        project_3=set(),
                    ),
                    hom_samples=hl.Struct(
                        project_1=set(),
                        project_2={'b'},
                        project_3=set(),
                    ),
                ),
                hl.Struct(
                    id=3,
                    ref_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3=set(),
                    ),
                    het_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3={'c'},
                    ),
                    hom_samples=hl.Struct(
                        project_1=set(),
                        project_2=set(),
                        project_3={'d', 'e'},
                    ),
                ),
            ],
        )
