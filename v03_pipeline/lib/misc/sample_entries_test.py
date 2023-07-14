import unittest

import hail as hl

from v03_pipeline.lib.misc.sample_entries import (
    deglobalize_sample_ids,
    globalize_sample_ids,
    union_entries_hts,
)


class SampleEntriesTest(unittest.TestCase):
    def test_globalize_and_deglobalize(self) -> None:
        entries_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                entries=hl.tarray(hl.tstruct(a=hl.tint32, s=hl.tstr)),
            ),
            key='id',
        )
        entries_ht = globalize_sample_ids(entries_ht)
        self.assertCountEqual(
            entries_ht.sample_ids.collect(),
            [
                [],
            ],
        )
        entries_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'HIGH_SR_BACKGROUND', 'UNRESOLVED'},
                    'entries': [
                        hl.Struct(a=1, s='a'),
                        hl.Struct(a=2, s='c'),
                        hl.Struct(a=1, s='e'),
                        hl.Struct(a=2, s='f'),
                    ],
                },
                {
                    'id': 1,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'entries': [
                        hl.Struct(a=2, s='a'),
                        hl.Struct(a=3, s='c'),
                        hl.Struct(a=4, s='e'),
                        hl.Struct(a=5, s='f'),
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                entries=hl.tarray(hl.tstruct(a=hl.tint32, s=hl.tstr)),
            ),
            key='id',
        )
        entries_ht = globalize_sample_ids(entries_ht)
        self.assertCountEqual(
            entries_ht.sample_ids.collect(),
            [
                ['a', 'c', 'e', 'f'],
            ],
        )
        self.assertCountEqual(
            entries_ht.entries.collect(),
            [
                [
                    hl.Struct(a=1),
                    hl.Struct(a=2),
                    hl.Struct(a=1),
                    hl.Struct(a=2),
                ],
                [
                    hl.Struct(a=2),
                    hl.Struct(a=3),
                    hl.Struct(a=4),
                    hl.Struct(a=5),
                ],
            ],
        )

        entries_ht = deglobalize_sample_ids(entries_ht)
        self.assertCountEqual(
            entries_ht.entries.collect(),
            [
                [
                    hl.Struct(a=1, s='a'),
                    hl.Struct(a=2, s='c'),
                    hl.Struct(a=1, s='e'),
                    hl.Struct(a=2, s='f'),
                ],
                [
                    hl.Struct(a=2, s='a'),
                    hl.Struct(a=3, s='c'),
                    hl.Struct(a=4, s='e'),
                    hl.Struct(a=5, s='f'),
                ],
            ],
        )

    def test_union_entries_hts_empty_current_table(self) -> None:
        entries_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                entries=hl.tarray(hl.tstruct(a=hl.tint32)),
            ),
            key='id',
            globals=hl.Struct(sample_ids=hl.empty_array(hl.tstr)),
        )
        callset_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'HIGH_SR_BACKGROUND', 'UNRESOLVED'},
                    'entries': [
                        hl.Struct(a=9),
                        hl.Struct(a=10),
                    ],
                },
                {
                    'id': 2,
                    'filters': {'HIGH_SR_BACKGROUND'},
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
        ht = union_entries_hts(entries_ht, callset_ht)
        self.assertCountEqual(
            ht.entries.collect(),
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

    def test_union_entries_hts(self) -> None:
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
        ht = union_entries_hts(entries_ht, callset_ht)
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(sample_ids=['a', 'c', 'e', 'f', 'b', 'g'])
            ]
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
