import unittest

import hail as hl

from v03_pipeline.lib.misc.lookup import (
    compute_callset_lookup_ht,
    join_lookup_hts,
    remove_family_guids,
    remove_project,
)
from v03_pipeline.lib.model import DatasetType, SampleType


class LookupTest(unittest.TestCase):
    def test_compute_callset_lookup_ht(self) -> None:
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
        lookup_ht = compute_callset_lookup_ht(
            DatasetType.MITO,
            mt,
            'project_a',
            SampleType.WES,
        )
        self.assertCountEqual(
            lookup_ht.globals.collect(),
            [
                hl.Struct(
                    family_samples={'1': ['b', 'c', 'd'], '2': ['a'], '3': ['e']},
                    project_sample_types=[('project_a', 'WES')],
                    project_families={('project_a', 'WES'): ['1', '2', '3']},
                ),
            ],
        )
        self.assertCountEqual(
            lookup_ht.collect(),
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

    def test_remove_family_guids(self) -> None:
        lookup_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'project_stats': [
                        [
                            None,
                            None,
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
                            None,
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
                project_sample_types=[('project_a', 'WES'), ('project_b', 'WGS')],
                project_families={
                    ('project_a', 'WES'): ['1', '2', '3'],
                    ('project_b', 'WGS'): ['4'],
                },
            ),
        )
        lookup_ht = remove_family_guids(
            lookup_ht,
            'project_c',
            SampleType.WES,
            hl.set(['2']),
        )
        lookup_ht = remove_family_guids(
            lookup_ht,
            'project_a',
            SampleType.WES,
            hl.set(['3', '1']),
        )
        lookup_ht = remove_family_guids(
            lookup_ht,
            'project_a',
            SampleType.WES,
            hl.set(['1']),
        )
        lookup_ht = remove_family_guids(
            lookup_ht,
            'project_b',
            SampleType.WGS,
            hl.set(['4']),
        )
        self.assertCountEqual(
            lookup_ht.globals.collect(),
            [
                hl.Struct(
                    project_sample_types=[('project_a', 'WES'), ('project_b', 'WGS')],
                    project_families={
                        ('project_a', 'WES'): ['2'],
                        ('project_b', 'WGS'): [],
                    },
                ),
            ],
        )
        self.assertCountEqual(
            lookup_ht.collect(),
            [
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

    def test_remove_project(self) -> None:
        lookup_ht = hl.Table.parallelize(
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
                        None,
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
                project_sample_types=[('project_a', 'WES'), ('project_b', 'WGS')],
                project_families={
                    ('project_a', 'WES'): ['1', '2', '3'],
                    ('project_b', 'WGS'): ['4'],
                },
            ),
        )
        lookup_ht = remove_project(
            lookup_ht,
            'project_c',
            SampleType.WES,
        )
        lookup_ht = remove_project(
            lookup_ht,
            'project_a',
            SampleType.WES,
        )
        self.assertCountEqual(
            lookup_ht.globals.collect(),
            [
                hl.Struct(
                    project_sample_types=[('project_b', 'WGS')],
                    project_families={('project_b', 'WGS'): ['4']},
                ),
            ],
        )
        self.assertCountEqual(
            lookup_ht.collect(),
            [
                hl.Struct(
                    id=1,
                    project_stats=[
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                        ],
                    ],
                ),
            ],
        )

    def test_join_lookup_hts_empty_table(self) -> None:
        ht = hl.Table.parallelize(
            [],
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
                project_sample_types=hl.empty_array(hl.ttuple(hl.tstr, hl.tstr)),
                project_families=hl.empty_dict(
                    hl.ttuple(hl.tstr, hl.tstr),
                    hl.tarray(hl.tstr),
                ),
            ),
        )
        callset_ht = hl.Table.parallelize(
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
                project_sample_types=[('project_a', 'WES')],
                project_families={('project_a', 'WES'): ['1', '2', '3']},
            ),
        )
        ht = join_lookup_hts(
            ht,
            callset_ht,
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    project_sample_types=[('project_a', 'WES')],
                    project_families={('project_a', 'WES'): ['1', '2', '3']},
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    project_stats=[
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
                    ],
                ),
                hl.Struct(
                    id=1,
                    project_stats=[
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
                    ],
                ),
            ],
        )

    def test_join_lookup_hts_new_project(self) -> None:
        ht = hl.Table.parallelize(
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
                        ],
                        [
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
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
                        ],
                        [
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
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
                project_sample_types=[('project_a', 'WES'), ('project_b', 'WGS')],
                project_families={
                    ('project_a', 'WES'): ['1', '2'],
                    ('project_b', 'WGS'): ['3'],
                },
            ),
        )
        callset_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                            hl.Struct(
                                ref_samples=4,
                                heteroplasmic_samples=4,
                                homoplasmic_samples=4,
                            ),
                        ],
                    ],
                },
                {
                    'id': 2,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                            hl.Struct(
                                ref_samples=4,
                                heteroplasmic_samples=4,
                                homoplasmic_samples=4,
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
                project_sample_types=[('project_c', 'WES')],
                project_families={('project_c', 'WES'): ['1', '2']},
            ),
        )
        ht = join_lookup_hts(
            ht,
            callset_ht,
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    project_sample_types=[
                        ('project_a', 'WES'),
                        ('project_b', 'WGS'),
                        ('project_c', 'WES'),
                    ],
                    project_families={
                        ('project_a', 'WES'): ['1', '2'],
                        ('project_b', 'WGS'): ['3'],
                        ('project_c', 'WES'): ['1', '2'],
                    },
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    project_stats=[
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
                        ],
                        [
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
                            hl.Struct(
                                ref_samples=4,
                                heteroplasmic_samples=4,
                                homoplasmic_samples=4,
                            ),
                        ],
                    ],
                ),
                hl.Struct(
                    id=1,
                    project_stats=[
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
                        ],
                        [
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
                            ),
                        ],
                        None,
                    ],
                ),
                hl.Struct(
                    id=2,
                    project_stats=[
                        None,
                        None,
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                            hl.Struct(
                                ref_samples=4,
                                heteroplasmic_samples=4,
                                homoplasmic_samples=4,
                            ),
                        ],
                    ],
                ),
            ],
        )

    def test_join_lookup_hts_existing_project(self) -> None:
        ht = hl.Table.parallelize(
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
                        ],
                        [
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
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
                        ],
                        [
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
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
                project_sample_types=[('project_a', 'WES'), ('project_b', 'WGS')],
                project_families={
                    ('project_a', 'WES'): ['1', '2'],
                    ('project_b', 'WGS'): ['3'],
                },
            ),
        )
        callset_ht = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                            hl.Struct(
                                ref_samples=4,
                                heteroplasmic_samples=4,
                                homoplasmic_samples=4,
                            ),
                        ],
                    ],
                },
                {
                    'id': 2,
                    'project_stats': [
                        [
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                            hl.Struct(
                                ref_samples=4,
                                heteroplasmic_samples=4,
                                homoplasmic_samples=4,
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
                project_sample_types=[('project_b', 'WGS')],
                project_families={('project_b', 'WGS'): ['1', '2']},
            ),
        )
        ht = join_lookup_hts(
            ht,
            callset_ht,
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
                hl.Struct(
                    project_sample_types=[('project_a', 'WES'), ('project_b', 'WGS')],
                    project_families={
                        ('project_a', 'WES'): ['1', '2'],
                        ('project_b', 'WGS'): ['3', '1', '2'],
                    },
                ),
            ],
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    project_stats=[
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
                        ],
                        [
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
                            ),
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                            hl.Struct(
                                ref_samples=4,
                                heteroplasmic_samples=4,
                                homoplasmic_samples=4,
                            ),
                        ],
                    ],
                ),
                hl.Struct(
                    id=1,
                    project_stats=[
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
                        ],
                        [
                            hl.Struct(
                                ref_samples=2,
                                heteroplasmic_samples=2,
                                homoplasmic_samples=2,
                            ),
                            None,
                            None,
                        ],
                    ],
                ),
                hl.Struct(
                    id=2,
                    project_stats=[
                        None,
                        [
                            None,
                            hl.Struct(
                                ref_samples=3,
                                heteroplasmic_samples=3,
                                homoplasmic_samples=3,
                            ),
                            hl.Struct(
                                ref_samples=4,
                                heteroplasmic_samples=4,
                                homoplasmic_samples=4,
                            ),
                        ],
                    ],
                ),
            ],
        )
