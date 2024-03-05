import unittest
from unittest import mock

import hail as hl

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.reference_data.compare_globals import (
    Globals,
    get_datasets_to_update,
)

CONFIG = {
    'a': {
        '38': {
            'custom_import': None,
            'source_path': 'a_path',  # 'a' has a custom import
            'select': {
                'test_select': 'info.test_select',
                'test_enum': 'test_enum',
            },
            'version': 'a_version',
            'enum_select': {'test_enum': ['A', 'B']},
        },
    },
    'b': {  # b is missing version
        '38': {
            'path': 'b_path',
            'select': {
                'test_select': 'info.test_select',
                'test_enum': 'test_enum',
            },
            'enum_select': {'test_enum': ['C', 'D']},
            'custom_select': lambda ht: {'field2': ht.info.test_select_2},
        },
    },
}

B_TABLE = hl.Table.parallelize(
    [],
    schema=hl.tstruct(
        locus=hl.tlocus('GRCh38'),
        alleles=hl.tarray(hl.tstr),
        info=hl.tstruct(
            test_select=hl.tint,
            test_select_2=hl.tint,
        ),
        test_enum=hl.tstr,
    ),
    globals=hl.Struct(
        version='b_version',
        path='b_path',
        enums=hl.Struct(test_enum=['C', 'D']),
    ),
    key=['locus', 'alleles'],
)


class CompareGlobalsTest(unittest.TestCase):
    @mock.patch.dict('v03_pipeline.lib.reference_data.compare_globals.CONFIG', CONFIG)
    @mock.patch(
        'v03_pipeline.lib.reference_data.compare_globals.import_ht_from_config_path',
    )
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_create_globals_from_dataset_configs(
        self,
        mock_rdc_datasets,
        mock_import_dataset_ht,
    ):
        mock_rdc_datasets.return_value = ['a', 'b']
        mock_import_dataset_ht.side_effect = [
            hl.Table.parallelize(
                [],
                schema=hl.tstruct(
                    locus=hl.tlocus('GRCh38'),
                    alleles=hl.tarray(hl.tstr),
                    info=hl.tstruct(
                        test_select=hl.tint,
                    ),
                    test_enum=hl.tstr,
                ),
                globals=hl.Struct(
                    version='a_version',
                    path='a_path',
                    enums=hl.Struct(test_enum=['A', 'B']),
                ),
                key=['locus', 'alleles'],
            ),
            B_TABLE,
        ]
        dataset_config_globals = Globals.from_dataset_configs(
            rdc=ReferenceDatasetCollection.INTERVAL,
            dataset_type=DatasetType.SNV_INDEL,
            reference_genome=ReferenceGenome.GRCh38,
        )
        self.assertTrue(
            dataset_config_globals.versions == {'a': 'a_version', 'b': 'b_version'},
        )
        self.assertTrue(
            dataset_config_globals.paths == {'a': 'a_path', 'b': 'b_path'},
        )
        self.assertTrue(
            dataset_config_globals.enums
            == {'a': {'test_enum': ['A', 'B']}, 'b': {'test_enum': ['C', 'D']}},
        )
        self.assertTrue(
            dataset_config_globals.selects
            == {
                'a': {'test_select', 'test_enum_id'},
                'b': {'test_select', 'field2', 'test_enum_id'},
            },
        )

    @mock.patch.dict('v03_pipeline.lib.reference_data.compare_globals.CONFIG', CONFIG)
    @mock.patch(
        'v03_pipeline.lib.reference_data.dataset_table_operations.hl.read_table',
    )
    def test_create_globals_from_dataset_configs_single_dataset(self, mock_read_table):
        mock_read_table.return_value = B_TABLE

        dataset_config_globals = Globals.from_dataset_configs(
            rdc=ReferenceDatasetCollection.COMBINED,
            dataset_type=DatasetType.SNV_INDEL,
            reference_genome=ReferenceGenome.GRCh38,
            datasets=['b'],
        )

        self.assertTrue(
            dataset_config_globals.versions == {'b': 'b_version'},
        )
        self.assertTrue(
            dataset_config_globals.paths == {'b': 'b_path'},
        )
        print(dataset_config_globals.enums)
        self.assertTrue(
            dataset_config_globals.enums == {'b': {'test_enum': ['C', 'D']}},
        )
        self.assertTrue(
            dataset_config_globals.selects
            == {
                'b': {'test_select', 'field2', 'test_enum_id'},
            },
        )

    def test_from_rdc_or_annotations_ht(self):
        rdc_ht = hl.Table.parallelize(
            [],
            schema=hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tarray(hl.tstr),
                gnomad_non_coding_constraint=hl.tstruct(
                    z_score=hl.tfloat32,
                ),
                screen=hl.tstruct(
                    region_type_ids=hl.tarray(hl.tint32),
                ),
            ),
            globals=hl.Struct(
                paths=hl.Struct(
                    gnomad_non_coding_constraint='gnomad_non_coding_constraint_path',
                    screen='screen_path',
                ),
                versions=hl.Struct(
                    gnomad_non_coding_constraint='v1',
                    screen='v2',
                ),
                enums=hl.Struct(
                    screen=hl.Struct(region_type=['C', 'D']),
                ),
            ),
        )
        rdc_globals = Globals.from_ht(
            rdc_ht,
            rdc=ReferenceDatasetCollection.INTERVAL,
            dataset_type=DatasetType.SNV_INDEL,
        )
        self.assertTrue(
            rdc_globals.versions
            == {'gnomad_non_coding_constraint': 'v1', 'screen': 'v2'},
        )
        self.assertTrue(
            rdc_globals.paths
            == {
                'gnomad_non_coding_constraint': 'gnomad_non_coding_constraint_path',
                'screen': 'screen_path',
            },
        )
        self.assertTrue(
            rdc_globals.enums == {'screen': {'region_type': ['C', 'D']}},
        )
        self.assertTrue(
            rdc_globals.selects
            == {
                'gnomad_non_coding_constraint': {'z_score'},
                'screen': {'region_type_ids'},
            },
        )

    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_get_datasets_to_update_version_different(self, mock_rdc_datasets):
        mock_rdc_datasets.return_value = ['a', 'b', 'c']
        result = get_datasets_to_update(
            rdc=ReferenceDatasetCollection.INTERVAL,
            ht1_globals=Globals(
                paths={'a': 'a_path', 'b': 'b_path'},
                # 'a' has a different version, 'c' is missing version in ht2_globals
                versions={'a': 'v2', 'b': 'v2', 'c': 'v1'},
                enums={'a': {}, 'b': {}, 'c': {}},
                selects={'a': set(), 'b': set()},
            ),
            ht2_globals=Globals(
                paths={'a': 'a_path', 'b': 'b_path'},
                versions={'a': 'v1', 'b': 'v2'},
                enums={'a': {}, 'b': {}},
                selects={'a': set(), 'b': set()},
            ),
            dataset_type=DatasetType.SNV_INDEL,
        )
        self.assertTrue(result == ['a', 'c'])

    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_get_datasets_to_update_path_different(self, mock_rdc_datasets):
        mock_rdc_datasets.return_value = ['a', 'b', 'c']
        result = get_datasets_to_update(
            rdc=ReferenceDatasetCollection.INTERVAL,
            ht1_globals=Globals(
                # 'b' has a different path, 'c' is missing path in ht2_globals
                paths={'a': 'a_path', 'b': 'old_b_path', 'c': 'extra_c_path'},
                versions={'a': 'v1', 'b': 'v2'},
                enums={'a': {}, 'b': {}},
                selects={'a': set(), 'b': set()},
            ),
            ht2_globals=Globals(
                paths={'a': 'a_path', 'b': 'b_path'},
                versions={'a': 'v1', 'b': 'v2'},
                enums={'a': {}, 'b': {}},
                selects={'a': set(), 'b': set()},
            ),
            dataset_type=DatasetType.SNV_INDEL,
        )
        self.assertTrue(result == ['b', 'c'])

    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_get_datasets_to_update_enum_different(self, mock_rdc_datasets):
        mock_rdc_datasets.return_value = ['a', 'b', 'c']
        result = get_datasets_to_update(
            rdc=ReferenceDatasetCollection.INTERVAL,
            ht1_globals=Globals(
                paths={'a': 'a_path', 'b': 'b_path'},
                versions={'a': 'v1', 'b': 'v2'},
                # 'a' has different enum values, 'b' has different enum key, 'c' is missing enum in ht2_globals
                enums={
                    'a': {'test_enum': ['A', 'B']},
                    'b': {'enum_key_1': []},
                    'c': {},
                },
                selects={'a': set(), 'b': set()},
            ),
            ht2_globals=Globals(
                paths={'a': 'a_path', 'b': 'b_path'},
                versions={'a': 'v1', 'b': 'v2'},
                enums={'a': {'test_enum': ['C', 'D']}, 'b': {'enum_key_2': []}},
                selects={'a': set(), 'b': set()},
            ),
            dataset_type=DatasetType.SNV_INDEL,
        )
        self.assertTrue(result == ['a', 'b', 'c'])

    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_get_datasets_to_update_select_different(self, mock_rdc_datasets):
        mock_rdc_datasets.return_value = ['a', 'b', 'c']
        result = get_datasets_to_update(
            rdc=ReferenceDatasetCollection.INTERVAL,
            ht1_globals=Globals(
                paths={'a': 'a_path', 'b': 'b_path'},
                versions={'a': 'v1', 'b': 'v2'},
                enums={'a': {}, 'b': {}},
                # 'a' has extra select, 'b' has different select, 'c' is missing select in ht2_globals
                selects={
                    'a': {'field1', 'field2'},
                    'b': {'test_select'},
                    'c': set('test_select'),
                },
            ),
            ht2_globals=Globals(
                paths={'a': 'a_path', 'b': 'b_path'},
                versions={'a': 'v1', 'b': 'v2'},
                enums={'a': {}, 'b': {}},
                selects={'a': {'field1'}, 'b': {'test_select_2'}},
            ),
            dataset_type=DatasetType.SNV_INDEL,
        )
        self.assertTrue(result == ['a', 'b', 'c'])
