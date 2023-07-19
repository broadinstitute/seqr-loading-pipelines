import unittest

import hail as hl

from v03_pipeline.lib.misc.sample_entries import (
    deglobalize_sample_ids,
    filter_callset_entries,
    filter_hom_ref_rows,
    globalize_sample_ids,
    join_entries_hts,
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

    def test_join_entries_hts_empty_current_table(self) -> None:
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
        ht = join_entries_hts(entries_ht, callset_ht)
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

    def test_filter_callset_entries(self) -> None:
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
        sample_subset_ht = hl.Table.parallelize(
            [{'s': 'a'}, {'s': 'f'}],
            hl.tstruct(
                s=hl.dtype('str'),
            ),
            key='s',
        )
        ht = filter_callset_entries(entries_ht, sample_subset_ht)
        self.assertCountEqual(
            ht.globals.collect(),
            [hl.Struct(sample_ids=['c', 'e'])],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    filters={'HIGH_SR_BACKGROUND'},
                    entries=[
                        hl.Struct(a=2),
                        hl.Struct(a=1),
                    ],
                ),
                hl.Struct(
                    id=1,
                    filters={'HIGH_SR_BACKGROUND'},
                    entries=[
                        hl.Struct(a=3),
                        hl.Struct(a=4),
                    ],
                ),
            ],
        )

    def test_filter_hom_ref_rows(self) -> None:
        entries_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'entries': [
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                    ],
                },
                {
                    'id': 1,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'entries': [
                        hl.missing(hl.tstruct(GT=hl.tcall)),
                        hl.missing(hl.tstruct(GT=hl.tcall)),
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                    ],
                },
                {
                    'id': 2,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'entries': [
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[1, 0], phased=False)),
                        hl.missing(hl.tstruct(GT=hl.tcall)),
                        hl.missing(hl.tstruct(GT=hl.tcall)),
                    ],
                },
                {
                    'id': 3,
                    'filters': {'HIGH_SR_BACKGROUND'},
                    'entries': [
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[1, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[1, 1], phased=False)),
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                filters=hl.tset(hl.tstr),
                entries=hl.tarray(hl.tstruct(GT=hl.tcall)),
            ),
            key='id',
            globals=hl.Struct(sample_ids=['a', 'b', 'c', 'd']),
        )
        ht = filter_hom_ref_rows(entries_ht)
        self.assertCountEqual(
            ht.globals.collect(),
            [hl.Struct(sample_ids=['a', 'b', 'c', 'd'])],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=2,
                    filters={'HIGH_SR_BACKGROUND'},
                    entries=[
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[1, 0], phased=False)),
                        None,
                        None,
                    ],
                ),
                hl.Struct(
                    id=3,
                    filters={'HIGH_SR_BACKGROUND'},
                    entries=[
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[0, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[1, 0], phased=False)),
                        hl.Struct(GT=hl.Call(alleles=[1, 1], phased=False)),
                    ],
                ),
            ],
        )
