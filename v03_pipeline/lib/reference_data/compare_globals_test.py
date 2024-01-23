import unittest
from unittest import mock

import hail as hl
from hail.utils import HailUserError

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_data.compare_globals import (
    ReferenceDataGlobals,
    get_datasets_to_update,
    ht_enums_match_config,
    ht_path_matches_config,
    ht_selects_match_config,
    ht_version_matches_config,
)

MOCK_JOINED_REFERENCE_DATA_HT = hl.Table.parallelize(
    [
        {
            'locus': hl.Locus(
                contig='chr1',
                position=1,
                reference_genome='GRCh38',
            ),
            'alleles': ['A', 'C'],
            'a': hl.Struct(d=1),
            'b': hl.Struct(e=2),
        },
    ],
    hl.tstruct(
        locus=hl.tlocus('GRCh38'),
        alleles=hl.tarray(hl.tstr),
        a=hl.tstruct(d=hl.tint32),
        b=hl.tstruct(e=hl.tint32),
    ),
    key=['locus', 'alleles'],
    globals=hl.Struct(
        paths=hl.Struct(
            a='a_path',
            b='b_path',
        ),
        versions=hl.Struct(
            a='a_version',
            b='b_version',
        ),
        enums=hl.Struct(
            a=hl.Struct(),
            b=hl.Struct(),
        ),
    ),
)


class CompareGlobalsTest(unittest.TestCase):
    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.compare_globals.CONFIG',
        {
            'a': {
                '38': {
                    'path': 'a_path',
                    'select': ['d'],
                    'version': 'a_version',
                },
            },
        },
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.compare_globals.import_ht_from_config_path',
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.compare_globals.parse_dataset_version',
    )
    def test_get_datasets_to_update_no_updates(
        self,
        mock_parse_dataset_version,
        mock_import_ht_from_config_path,
    ):
        """Dataset a has the same globals as the config, so it should not be updated."""
        mock_parse_dataset_version.return_value = 'a_version'
        # set dataset ht to None because it doesn't need to be checked for this test
        mock_import_ht_from_config_path.side_effect = None

        result = get_datasets_to_update(
            MOCK_JOINED_REFERENCE_DATA_HT,
            datasets=['a'],
            reference_genome=ReferenceGenome.GRCh38,
        )
        self.assertEqual(result, [])

    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.compare_globals.CONFIG',
        {
            'a': {
                '38': {
                    'path': 'a_path',
                    'version': 'new_version',
                },
            },
            'c': {
                '38': {
                    'path': 'c_path',
                    'version': 'c_version',
                },
            },
        },
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.compare_globals.import_ht_from_config_path',
    )
    @mock.patch(
        'v03_pipeline.lib.reference_data.compare_globals.parse_dataset_version',
    )
    def test_get_datasets_to_update_has_updates(
        self,
        mock_parse_dataset_version,
        mock_import_ht_from_config_path,
    ):
        """Dataset a has a new version, and c is not in the joined table, so they should be updated."""
        mock_parse_dataset_version.return_value = 'new_version'
        # set dataset ht to None because it doesn't need to be checked for this test
        mock_import_ht_from_config_path.side_effect = None

        result = get_datasets_to_update(
            MOCK_JOINED_REFERENCE_DATA_HT,
            datasets=['a', 'c'],
            reference_genome=ReferenceGenome.GRCh38,
        )
        self.assertEqual(result, ['a', 'c'])

    def test_ht_version_matches_config_joined_ht_version_missing(self):
        """If the joined_ht has no version for a given dataset in globals, return False."""
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(
                    a='v1',
                ),
                paths=hl.Struct(),
                enums=hl.Struct(),
            ),
        )
        dataset_b_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            globals=hl.Struct(version='v1'),
        )
        result = ht_version_matches_config(joined_ht_globals, 'b', {}, dataset_b_ht)
        self.assertFalse(result)

    @mock.patch(
        'v03_pipeline.lib.reference_data.compare_globals.parse_dataset_version',
    )
    def test_ht_version_matches_config(
        self,
        mock_parse_dataset_version,
    ):
        """If the joined_ht version matches the config version for a given dataset, return True."""
        dataset_a_config = {'version': 'v1', 'path': 'mock_path'}
        mock_parse_dataset_version.return_value = 'v1'

        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(
                    a='v1',
                ),
                paths=hl.Struct(),
                enums=hl.Struct(),
            ),
        )
        dataset_a_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            globals=hl.Struct(version='v1'),
        )
        result = ht_version_matches_config(
            joined_ht_globals,
            'a',
            dataset_a_config,
            dataset_a_ht,
        )
        self.assertTrue(result)

    def test_ht_version_matches_config_use_dataset_ht_version(self):
        """If the config version for a given dataset is missing, use the dataset_ht version to compare."""
        dataset_a_config = {'path': 'mock_path'}
        dataset_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            globals=hl.Struct(version='v2'),
        )

        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(
                    a='v1',
                ),
                paths=hl.Struct(),
                enums=hl.Struct(),
            ),
        )
        result = ht_version_matches_config(
            joined_ht_globals,
            'a',
            dataset_a_config,
            dataset_ht,
        )
        self.assertFalse(result)

    def test_ht_version_matches_config_handles_dataset_version_mismatch(
        self,
    ):
        """If the dataset_ht version does not match the config version, HailUserError should be raised."""
        dataset_a_config = {'path': 'mock_path', 'version': 'v1'}
        dataset_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            globals=hl.Struct(version='v2'),
        )

        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(
                    a='v1',
                ),
                paths=hl.Struct(),
                enums=hl.Struct(),
            ),
        )
        with self.assertRaises(HailUserError):
            ht_version_matches_config(
                joined_ht_globals,
                'a',
                dataset_a_config,
                dataset_ht,
            )

    def test_ht_path_matches_config_joined_ht_path_missing(self):
        """If the joined_ht has no path in globals, return False."""
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(),
                paths=hl.Struct(
                    a='gs://a_path.ht',
                ),
                enums=hl.Struct(),
            ),
        )
        result = ht_path_matches_config(joined_ht_globals, 'b', {})
        self.assertFalse(result)

    def test_ht_path_matches_config_no_match(self):
        """If the joined_ht path does not match the config path, return False."""
        dataset_a_config = {'path': 'gs://a_path.ht'}
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(),
                paths=hl.Struct(
                    a='gs://a_path_OLD.ht',
                ),
                enums=hl.Struct(),
            ),
        )
        result_a = ht_path_matches_config(
            joined_ht_globals,
            'a',
            dataset_a_config,
        )
        self.assertFalse(result_a)

    def test_ht_path_matches_config_custom_import(self):
        """If the config has a custom_import, check that the source_path matches instead."""
        dataset_b_config = {
            'custom_import': None,
            'source_path': 'gs://b_path.mt',
        }
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(),
                paths=hl.Struct(
                    b='gs://b_path.mt',
                ),
                enums=hl.Struct(),
            ),
        )
        result_b = ht_path_matches_config(
            joined_ht_globals,
            'b',
            dataset_b_config,
        )
        self.assertTrue(result_b)

    def test_ht_enums_match_config_both_missing(self):
        """If the joined_ht has no enums in globals for that dataset nor in the config, return True."""
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(),
                paths=hl.Struct(),
                enums=hl.Struct(
                    a=hl.Struct(test_enum=['A', 'B']),
                ),
            ),
        )
        result = ht_enums_match_config(joined_ht_globals, 'b', {})
        self.assertTrue(result)

    def test_ht_enums_match_config_joined_ht_missing(self):
        """If the joined_ht has no enums in globals for that dataset but does in the config, return False."""
        dataset_config = {'enum_select': {'test_enum': ['B']}}
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(),
                paths=hl.Struct(),
                enums=hl.Struct(
                    a=hl.Struct(test_enum=['A', 'B']),
                ),
            ),
        )
        result = ht_enums_match_config(joined_ht_globals, 'b', dataset_config)
        self.assertFalse(result)

    def test_ht_enums_match_config(self):
        """If the joined_ht enums match the config enums, return True."""
        dataset_config = {
            'enum_select': {
                'test_enum': ['A', 'B'],
            },
        }
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(),
                paths=hl.Struct(),
                enums=hl.Struct(
                    a=hl.Struct(test_enum=['A', 'B']),
                ),
            ),
        )
        result = ht_enums_match_config(joined_ht_globals, 'a', dataset_config)
        self.assertTrue(result)

    def test_ht_enums_match_config_key_does_not_match(self):
        """If the joined_ht enum keys do not match the config enums, return False."""
        dataset_config = {
            'enum_select': {
                'new_enum': ['A', 'B'],
            },
        }
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(),
                paths=hl.Struct(),
                enums=hl.Struct(
                    a=hl.Struct(old_enum=['A', 'B']),
                ),
            ),
        )
        result = ht_enums_match_config(joined_ht_globals, 'a', dataset_config)
        self.assertFalse(result)

    def test_ht_enums_match_config_values_do_not_match(self):
        """If the joined_ht enum values do not match the config enum values, return False."""
        dataset_config = {
            'enum_select': {
                'test_enum': ['A', 'B', 'C'],
            },
        }
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(),
                paths=hl.Struct(),
                enums=hl.Struct(
                    a=hl.Struct(test_enum=['A', 'B']),
                ),
            ),
        )
        result = ht_enums_match_config(joined_ht_globals, 'a', dataset_config)
        self.assertFalse(result)

    def test_ht_selects_match_config_no_match_extra_field(self):
        """If the joined_ht has no matching field in the config/dataset_ht, return False."""
        dataset_config = {
            'select': {'field1': 'info.a', 'field2': 'info.c'},
        }
        # joined_ht has no 'field2' field
        joined_ht = hl.Table.parallelize(
            [
                {'a': hl.Struct(field1=1)},
            ],
            schema=hl.tstruct(
                a=hl.tstruct(field1=hl.tint32),
            ),
        )
        # dataset_ht has extra field 'info.c'
        dataset_ht = hl.Table.parallelize(
            [
                {'info': hl.Struct(a=1, c=1)},
            ],
            schema=hl.tstruct(
                info=hl.tstruct(a=hl.tint32, c=hl.tint32),
            ),
        )
        result = ht_selects_match_config(joined_ht, 'a', dataset_config, dataset_ht)
        self.assertFalse(result)

    def test_ht_selects_match_config_no_match_extra_ht_field(self):
        """If the config/dataset_ht is missing a field from the joined_ht, return False."""
        dataset_config = {
            'select': {'field1': 'info.a'},
        }
        # joined_ht has no 'field2' field
        joined_ht = hl.Table.parallelize(
            [
                {'a': hl.Struct(field1=1, field2=1)},
            ],
            schema=hl.tstruct(
                a=hl.tstruct(field1=hl.tint32, field2=hl.tint32),
            ),
        )
        # 'field2' is present in the config and on the dataset_ht
        dataset_ht = hl.Table.parallelize(
            [
                {'info': hl.Struct(a=1)},
            ],
            schema=hl.tstruct(
                info=hl.tstruct(a=hl.tint32),
            ),
        )
        result = ht_selects_match_config(joined_ht, 'a', dataset_config, dataset_ht)
        self.assertFalse(result)

    def test_ht_selects_match_config_custom_select(self):
        """If the config has a custom_select, account for those additional fields."""
        dataset_config = {
            'select': {'field1': 'info.a'},
            'custom_select': lambda ht: {'field2': ht.info.b},
        }
        # both fields are on the joined_ht
        joined_ht = hl.Table.parallelize(
            [
                {'a': hl.Struct(field1=1, field2=1)},
            ],
            schema=hl.tstruct(
                a=hl.tstruct(field1=hl.tint32, field2=hl.tint32),
            ),
        )
        # both fields are on the dataset_ht
        dataset_ht = hl.Table.parallelize(
            [
                {'info': hl.Struct(a=1, b=1)},
            ],
            schema=hl.tstruct(
                info=hl.tstruct(a=hl.tint32, b=hl.tint32),
            ),
        )
        result = ht_selects_match_config(joined_ht, 'a', dataset_config, dataset_ht)
        self.assertTrue(result)
