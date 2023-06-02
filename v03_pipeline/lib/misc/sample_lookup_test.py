import unittest

import hail as hl

from v03_pipeline.lib.misc.sample_lookup import (
    AC,
    AF,
    AN,
    homozygote_count,
    remove_callset_sample_ids,
    union_sample_lookup_hts,
)


class SampleLookupTest(unittest.TestCase):
    def test_allele_count_helpers(self) -> None:
        sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': {'a', 'c'},
                    'het_samples': {'b', 'd'},
                    'hom_samples': {'e', 'f'},
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
        sample_lookup_ht = sample_lookup_ht.select(
            AC=AC(sample_lookup_ht),
            AF=AF(sample_lookup_ht),
            AN=AN(sample_lookup_ht),
            homozygote_count=homozygote_count(sample_lookup_ht),
        )
        self.assertCountEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(id=0, AC=6, AF=0.5, AN=12, homozygote_count=2),
            ],
        )

    def test_remove_callset_sample_ids(self) -> None:
        sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': set(),
                    'het_samples': {'b', 'd', 'f'},
                    'hom_samples': {'e', 'f'},
                },
                {
                    'id': 1,
                    'ref_samples': {'f'},
                    'het_samples': {'a'},
                    'hom_samples': set(),
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
        )
        self.assertListEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples=set(),
                    het_samples={'b'},
                    hom_samples=set(),
                ),
                hl.Struct(
                    id=1,
                    ref_samples=set(),
                    het_samples={'a'},
                    hom_samples=set(),
                ),
            ],
        )

    def test_union_sample_lookup_hts(self) -> None:
        sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': set(),
                    'het_samples': {'b', 'd', 'f'},
                    'hom_samples': {'e', 'f'},
                },
                {
                    'id': 1,
                    'ref_samples': {'f'},
                    'het_samples': {'a'},
                    'hom_samples': set(),
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
        callset_sample_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'ref_samples': {'e'},
                    'het_samples': {'f', 'g'},
                    'hom_samples': set(),
                },
                {
                    'id': 2,
                    'ref_samples': {'c'},
                    'het_samples': {'d'},
                    'hom_samples': set(),
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
        sample_lookup_ht = union_sample_lookup_hts(
            sample_lookup_ht,
            callset_sample_lookup_ht,
        )
        self.assertCountEqual(
            sample_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples={'e'},
                    het_samples={'b', 'd', 'f', 'g'},
                    hom_samples={'e', 'f'},
                ),
                hl.Struct(
                    id=1,
                    ref_samples={'f'},
                    het_samples={'a'},
                    hom_samples=set(),
                ),
                hl.Struct(
                    id=2,
                    ref_samples={'c'},
                    het_samples={'d'},
                    hom_samples=set(),
                ),
            ],
        )
