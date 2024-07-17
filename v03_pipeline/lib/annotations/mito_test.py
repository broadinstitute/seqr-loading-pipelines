import unittest

import hail as hl

from v03_pipeline.lib.annotations.mito import gt_stats
from v03_pipeline.lib.model import DatasetType


class MITOTest(unittest.TestCase):
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
                            DatasetType.MITO.lookup_table_pack(hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
                            )),
                        ],
                        [],
                    ],
                },
                {
                    'id': 1,
                    'project_stats': [
                        [
                            DatasetType.MITO.lookup_table_pack(hl.Struct(
                                ref_samples=6,
                                heteroplasmic_samples=0,
                                homoplasmic_samples=0,
                            )),
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
                            buffer=hl.tint32,
                        ),
                    ),
                ),
            ),
            key='id',
            globals=hl.Struct(
                project_guids=['project_1', 'project_2'],
                project_families={'project_1': ['a'], 'project_2': []},
            ),
        )
        ht = ht.select(gt_stats=gt_stats(ht, lookup_ht))
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    gt_stats=hl.Struct(
                        AC_het=2,
                        AF_het=0.3333333432674408,
                        AC_hom=2,
                        AF_hom=0.3333333432674408,
                        AN=6,
                    ),
                ),
                hl.Struct(
                    id=1,
                    gt_stats=hl.Struct(
                        AC_het=0,
                        AF_het=0.0,
                        AC_hom=0,
                        AF_hom=0.0,
                        AN=6,
                    ),
                ),
            ],
        )
