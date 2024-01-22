import unittest
from unittest import mock

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_data.compare_globals import (
    ReferenceDataGlobals,
    ht_enums_match_config,
    ht_path_matches_config,
    ht_selects_match_config,
    ht_version_matches_config,
    validate_joined_ht_globals_match_config,
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
        'v03_pipeline.lib.reference_data.dataset_table_operations.hl.read_table',
    )
    def test_validate_joined_ht_globals_match_config(self, mock_read_table):
        dataset_ht_no_globals = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
        )
        mock_read_table.return_value = dataset_ht_no_globals

        result = validate_joined_ht_globals_match_config(
            MOCK_JOINED_REFERENCE_DATA_HT,
            dataset='a',
            reference_genome=ReferenceGenome.GRCh38,
        )
        self.assertTrue(result)

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
        result = ht_version_matches_config(
            joined_ht_globals,
            'b',
            {},
            reference_genome=ReferenceGenome.GRCh38,
        )
        self.assertFalse(result)

    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.hl.read_table',
    )
    def test_ht_version_matches_config(self, mock_read_table):
        """If the joined_ht version matches the config version for a given dataset, return True."""
        dataset_a_config = {'version': 'v1', 'path': 'mock_path'}
        joined_ht_globals = ReferenceDataGlobals(
            hl.Struct(
                versions=hl.Struct(
                    a='v1',
                ),
                paths=hl.Struct(),
                enums=hl.Struct(),
            ),
        )

        dataset_ht_no_globals = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
        )
        mock_read_table.return_value = dataset_ht_no_globals

        result = ht_version_matches_config(
            joined_ht_globals,
            'a',
            dataset_a_config,
            reference_genome=ReferenceGenome.GRCh38,
        )
        self.assertTrue(result)

    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.hl.read_table',
    )
    def test_ht_version_matches_config_use_dataset_ht_version(self, mock_read_table):
        """If the config version for a given dataset is missing, use the dataset_ht version."""
        dataset_a_config = {'path': 'mock_path'}

        dataset_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            globals=hl.Struct(version='v2'),
        )
        mock_read_table.return_value = dataset_ht

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
            reference_genome=ReferenceGenome.GRCh38,
        )
        self.assertFalse(result)

    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.hl.read_table',
    )
    def test_ht_version_matches_config_handles_dataset_version_mismatch(
        self,
        mock_read_table,
    ):
        """"""
        dataset_a_config = {'path': 'mock_path', 'version': 'v1'}

        dataset_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
            ),
            globals=hl.Struct(version='v2'),
        )
        mock_read_table.return_value = dataset_ht

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
            reference_genome=ReferenceGenome.GRCh38,
        )
        self.assertTrue(result)

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

    def test_ht_selects_match_config_no_match_extra_config_field(self):
        """If the joined_ht has no matching field in the config, return False."""
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
        result = ht_selects_match_config(joined_ht, 'a', dataset_config)
        self.assertFalse(result)

    def test_ht_selects_match_config_no_match_extra_ht_field(self):
        """If the config is missing a field from the joined_ht, return False."""
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
        result = ht_selects_match_config(joined_ht, 'a', dataset_config)
        self.assertFalse(result)

    def test_ht_selects_match_config_custom_select(self):
        """If the config has a custom_select, account for those additional fields."""
        dataset_config = {
            'select': {'field1': 'info.a'},
            'custom_select': None,
            'custom_select_keys': ['field2'],
        }
        joined_ht = hl.Table.parallelize(
            [
                {'a': hl.Struct(field1=1, field2=1)},
            ],
            schema=hl.tstruct(
                a=hl.tstruct(field1=hl.tint32, field2=hl.tint32),
            ),
        )
        result = ht_selects_match_config(joined_ht, 'a', dataset_config)
        self.assertTrue(result)
