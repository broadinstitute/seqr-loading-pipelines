import unittest

import hail as hl

from v03_pipeline.lib.misc.sample_entries import (
    deglobalize_ids,
    filter_new_callset_family_guids,
    globalize_ids,
    join_entries_hts,
)


class SampleEntriesTest(unittest.TestCase):
    def test_globalize_and_deglobalize(self) -> None:
        family_entries_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                family_entries=hl.tarray(hl.tarray(hl.tstruct(a=hl.tint32, s=hl.tstr))),
            ),
            key='id',
        )
        family_entries_ht = globalize_ids(family_entries_ht)
        self.assertCountEqual(
            family_entries_ht.sample_ids.collect(),
            [
                [],
            ],
        )
        family_entries_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'HIGH_SR_BACKGROUND', 'UNRESOLVED'},
                    'family_entries': [
                        [
                            hl.Struct(a=1, s='a', family_guid='123'),
                            hl.Struct(a=2, s='c', family_guid='123'),
                            hl.Struct(a=1, s='e', family_guid='123'),
                        ],
                        [
                            hl.Struct(a=2, s='f', family_guid='012'),
                        ],
                    ],
                },
                {
                    'id': 1,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'family_entries': [
                        [
                            hl.Struct(a=2, s='a', family_guid='123'),
                            hl.Struct(a=3, s='c', family_guid='123'),
                            hl.Struct(a=4, s='e', family_guid='123'),
                        ],
                        [
                            hl.Struct(a=5, s='f', family_guid='012'),
                        ],
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                family_entries=hl.tarray(hl.tarray(hl.tstruct(a=hl.tint32, s=hl.tstr))),
            ),
            key='id',
        )
        family_entries_ht = globalize_ids(family_entries_ht)
        self.assertCountEqual(
            family_entries_ht.family_guids.collect(),
            [
                ['012', '123'],
            ],
        )
        self.assertCountEqual(
            family_entries_ht.family_entries.collect(),
            [
                [
                    [
                        hl.Struct(a=2),
                    ],
                    [
                        hl.Struct(a=1),
                        hl.Struct(a=2),
                        hl.Struct(a=1),
                    ],
                ],
                [
                    [
                        hl.Struct(a=5),
                    ],
                    [
                        hl.Struct(a=2),
                        hl.Struct(a=3),
                        hl.Struct(a=4),
                    ],
                ],
            ],
        )
        family_entries_ht = deglobalize_ids(family_entries_ht)
        self.assertCountEqual(
            family_entries_ht.family_entries.collect(),
            [
                [
                    [
                        hl.Struct(a=2, s='a', family_guid='012'),
                    ],
                    [
                        hl.Struct(a=1, s='c', family_guid='123'),
                        hl.Struct(a=2, s='e', family_guid='123'),
                        hl.Struct(a=1, s='f', family_guid='123'),
                    ],
                ],
                [
                    [
                        hl.Struct(a=5, s='a', family_guid='012'),
                    ],
                    [
                        hl.Struct(a=2, s='c', family_guid='123'),
                        hl.Struct(a=3, s='e', family_guid='123'),
                        hl.Struct(a=4, s='f', family_guid='123'),
                    ],
                ],
            ],
        )

    def test_filter_new_callset_family_guids(self) -> None:
        family_entries_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'family_entries': [
                        [
                            hl.Struct(a=1),
                        ],
                        [
                            hl.Struct(a=2),
                            hl.Struct(a=1),
                            hl.Struct(a=2),
                        ],
                    ],
                },
                {
                    'id': 1,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'family_entries': [
                        [
                            hl.Struct(a=2),
                        ],
                        None,
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                family_entries=hl.array(hl.tarray(hl.tstruct(a=hl.tint32))),
            ),
            key='id',
            globals=hl.Struct(
                family_guids=['012', '123'],
                family_samples={
                    '012': ['a'],
                    '123': ['c', 'e', 'f'],
                },
            ),
        )
        family_entries_ht = filter_new_callset_family_guids(family_entries_ht, ['012'])
        self.assertCountEqual(
            family_entries_ht.globals.collect(),
            [
                hl.Struct(
                    family_guids=['123'],
                    family_samples={
                        '123': ['c', 'e', 'f'],
                    },
                ),
            ],
        )
        self.assertCountEqual(
            family_entries_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    filters={'HIGH_SR_BACKGROUND'},
                    family_entries=[
                        None,
                        [
                            hl.Struct(a=2),
                            hl.Struct(a=1),
                            hl.Struct(a=2),
                        ],
                    ],
                ),
                hl.Struct(
                    id=1,
                    filters={'HIGH_SR_BACKGROUND'},
                    family_entries=[
                        None,
                        None,
                    ],
                ),
            ],
        )

    def test_filter_all_callset_entries(self) -> None:
        family_entries_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'family_entries': [
                        [
                            hl.Struct(a=1),
                        ],
                        [
                            hl.Struct(a=2),
                            hl.Struct(a=1),
                            hl.Struct(a=2),
                        ],
                    ],
                },
                {
                    'id': 1,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'family_entries': [
                        [
                            hl.Struct(a=2),
                        ],
                        [
                            hl.Struct(a=3),
                            hl.Struct(a=4),
                            hl.Struct(a=5),
                        ],
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                family_entries=hl.array(hl.tarray(hl.tstruct(a=hl.tint32))),
            ),
            key='id',
            globals=hl.Struct(
                family_guids=['012', '123'],
                family_samples={
                    '012': ['a'],
                    '123': ['c', 'e', 'f'],
                },
            ),
        )
        ht = filter_new_callset_family_guids(family_entries_ht, ['012', '123'])
        self.assertCountEqual(
            ht.globals.collect(),
            [hl.Struct(family_guids=[], family_samples={})],
        )

    def test_join_entries_hts_empty_current_table(self) -> None:
        family_entries_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                family_entries=hl.tarray(hl.tarray(hl.tstruct(a=hl.tint32, s=hl.tstr))),
            ),
            key='id',
            globals=hl.Struct(
                family_guids=hl.empty_array(hl.tstr),
                family_samples=hl.empty_dict(hl.tstr, hl.tarray(hl.tstr)),
            ),
        )
        callset_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'HIGH_SR_BACKGROUND', 'UNRESOLVED'},
                    'family_entries': [
                        [
                            hl.Struct(a=9),
                            hl.Struct(a=10),
                        ],
                    ],
                },
                {
                    'id': 2,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'family_entries': [
                        [
                            hl.Struct(a=11),
                            hl.Struct(a=12),
                        ],
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                family_entries=hl.tarray(hl.tarray(hl.tstruct(a=hl.tint32))),
            ),
            key='id',
            globals=hl.Struct(
                family_guids=['1', '2'],
                family_samples={'1': ['b'], '2': ['g']},
            ),
        )
        ht = join_entries_hts(family_entries_ht, callset_ht)
        self.assertCountEqual(
            ht.family_entries.collect(),
            [
                [
                    hl.Struct(a=9),
                    hl.Struct(a=10),
                ],
                [
                    hl.Struct(a=11),
                    hl.Struct(a=12),
                ],
            ],
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    family_guids=['1', '2'], family_samples={'1': ['b'], '2': ['g']}
                )
            ],
        )

    def test_join_entries_hts(self) -> None:
        entries_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'entries': [
                        hl.Struct(a=1),
                        hl.Struct(a=2),
                        hl.Struct(a=1),
                        hl.Struct(a=2),
                    ],
                },
                {
                    'id': 1,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'entries': [
                        hl.Struct(a=2),
                        hl.Struct(a=3),
                        hl.Struct(a=4),
                        hl.Struct(a=5),
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                entries=hl.tarray(hl.tstruct(a=hl.tint32)),
            ),
            key='id',
            globals=hl.Struct(sample_ids=['a', 'c', 'e', 'f']),
        )
        callset_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'PASS'},
                    'entries': [
                        hl.Struct(a=9),
                        hl.Struct(a=10),
                    ],
                },
                {
                    'id': 2,
                    'filters': {'HIGH_SR_BACKGROUND', 'PASS'},
                    'entries': [
                        hl.Struct(a=11),
                        hl.Struct(a=12),
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                entries=hl.tarray(hl.tstruct(a=hl.tint32)),
            ),
            key='id',
            globals=hl.Struct(sample_ids=['b', 'g']),
        )
        ht = join_entries_hts(entries_ht, callset_ht)
        self.assertCountEqual(
            ht.globals.collect(),
            [hl.Struct(sample_ids=['a', 'c', 'e', 'f', 'b', 'g'])],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    filters={'PASS'},
                    entries=[
                        hl.Struct(a=1),
                        hl.Struct(a=2),
                        hl.Struct(a=1),
                        hl.Struct(a=2),
                        hl.Struct(a=9),
                        hl.Struct(a=10),
                    ],
                ),
                hl.Struct(
                    id=1,
                    filters={'HIGH_SR_BACKGROUND'},
                    entries=[
                        hl.Struct(a=2),
                        hl.Struct(a=3),
                        hl.Struct(a=4),
                        hl.Struct(a=5),
                        None,
                        None,
                    ],
                ),
                hl.Struct(
                    id=2,
                    filters={'PASS', 'HIGH_SR_BACKGROUND'},
                    entries=[
                        None,
                        None,
                        None,
                        None,
                        hl.Struct(a=11),
                        hl.Struct(a=12),
                    ],
                ),
            ],
        )
