import unittest

import hail as hl

from v03_pipeline.lib.annotations.snv_indel import gt_stats
from v03_pipeline.lib.model import DatasetType


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
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=2,
                                het_samples=2,
                                hom_samples=2,
                            ),
                        ],
                        [],
                    ],
                },
                {
                    'id': 1,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=6,
                                het_samples=0,
                                hom_samples=0,
                            ),
                        ],
                        [],
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                project_stats=hl.tarray(
                    hl.tarray(
                        hl.tstruct(
                            **{
                                field: hl.tint32
                                for field in DatasetType.SNV_INDEL.lookup_table_fields_and_genotype_filter_fns
                            },
                        ),
                    ),
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
