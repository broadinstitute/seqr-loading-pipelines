import unittest

import hail as hl

from v03_pipeline.lib.misc.family_lookup import (
    compute_callset_family_lookup_ht,
    remove_new_callset_family_guids,
)
from v03_pipeline.lib.model import DatasetType


class SampleLookupTest(unittest.TestCase):
    def test_compute_callset_family_lookup_ht(self) -> None:
        mt = hl.MatrixTable.from_parts(
            rows={'variants': [1, 2]},
            cols={'s': ['a', 'b', 'c', 'd', 'e']},
            entries={
                'HL': [
                    [0.0, hl.missing(hl.tfloat), 0.99, 0.01, 0.01],
                    [0.1, 0.2, 0.94, 0.99, 0.01],
                ],
            },
            globals={'family_samples': {'2': ['a'], '1': ['b', 'c', 'd'], '3': ['e']}},
        )
        family_lookup_ht = compute_callset_family_lookup_ht(
            DatasetType.MITO,
            mt,
            'project_a',
        )
        self.assertCountEqual(
            family_lookup_ht.globals.collect(),
            [
                hl.Struct(
                    family_samples={'1': ['b', 'c', 'd'], '2': ['a'], '3': ['e']},
                    project_guids=['project_a'],
                    project_families={'project_a': ['1', '2', '3']},
                ),
            ],
        )
        self.assertCountEqual(
            family_lookup_ht.collect(),
            [
                hl.Struct(
                    row_idx=0,
                    project_stats=[
                        [
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=0,
                                homoplasmic_samples=0,
                            ),
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=0,
                            ),
                        ],
                    ],
                ),
                hl.Struct(
                    row_idx=1,
                    project_stats=[
                        [
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=1,
                            ),
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=0,
                            ),
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=0,
                            ),
                        ],
                    ],
                ),
            ],
        )

    def test_remove_new_callset_family_guids(self) -> None:
        family_lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=0,
                                homoplasmic_samples=0,
                            ),
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
                            ),
                        ],
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                        ],
                    ],
                },
                {
                    'id': 1,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=0,
                                heteroplasmic_samples=0,
                                homoplasmic_samples=0,
                            ),
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
                            ),
                        ],
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                        ],
                    ],
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                project_stats=hl.tarray(
                    hl.tarray(
                        hl.tstruct(
                            ref_samples=hl.tint32,
                            heteroplasmic_samples=hl.tint32,
                            homoplasmic_samples=hl.tint32,
                        ),
                    ),
                ),
            ),
            key='id',
            globals=hl.Struct(
                project_guids=['project_a', 'project_b'],
                project_families={'project_a': ['1', '2', '3'], 'project_b': ['4']},
            ),
        )
        family_lookup_ht = remove_new_callset_family_guids(
            family_lookup_ht,
            'project_c',
            ['2'],
        )
        family_lookup_ht = remove_new_callset_family_guids(
            family_lookup_ht,
            'project_a',
            ['3', '1'],
        )
        family_lookup_ht = remove_new_callset_family_guids(
            family_lookup_ht,
            'project_b',
            ['4'],
        )
        self.assertCountEqual(
            family_lookup_ht.globals.collect(),
            [
                hl.Struct(
                    project_guids=['project_a', 'project_b'],
                    project_families={'project_a': ['2'], 'project_b': []},
                ),
            ],
        )
        self.assertCountEqual(
            family_lookup_ht.collect(),
            [
                hl.Struct(
                    id=0,
                    project_stats=[
                        [
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                        ],
                        [],
                    ],
                ),
                hl.Struct(
                    id=1,
                    project_stats=[
                        [
                            hl.Struct(
                                ref_samples=1,
                                heteroplasmic_samples=1,
                                homoplasmic_samples=1,
                            ),
                        ],
                        [],
                    ],
                ),
            ],
        )
