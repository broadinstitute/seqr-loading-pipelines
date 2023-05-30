import unittest

import hail as hl

from v03_pipeline.lib.misc.genotypes import (
    remove_callset_sample_ids,
    union_sample_lookup_hts,
)


class GenotypesTest(unittest.TestCase):
    def test_remove_callset_sample_ids(self):
        genotypes_ht = hl.Table.parallelize(
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
        genotypes_ht = remove_callset_sample_ids(
            genotypes_ht,
            samples_ht,
        )
        self.assertListEqual(
            genotypes_ht.collect(),
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

    def test_union_sample_lookup_hts(self):
        genotypes_ht = hl.Table.parallelize(
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
        callset_genotypes_ht = hl.Table.parallelize(
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
        genotypes_ht = union_sample_lookup_hts(genotypes_ht, callset_genotypes_ht)
        self.assertCountEqual(
            genotypes_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    ref_samples={'e'},
                    het_samples={'b', 'd', 'f'},
                    hom_samples={'e', 'f', 'g'},
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
