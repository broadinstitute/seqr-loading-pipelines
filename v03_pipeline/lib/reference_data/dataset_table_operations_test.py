import unittest
from datetime import datetime
from unittest import mock

import hail as hl
import pytz

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.reference_data.config import (
    dbnsfp_custom_select,
    dbnsfp_mito_custom_select,
)
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    get_dataset_ht,
    get_enum_select_fields,
    update_existing_joined_hts,
    update_or_create_joined_ht,
)

MOCK_CONFIG = {
    'a': {
        '38': {
            'path': '',
            'select': [
                'd',
            ],
        },
    },
    'b': {
        '38': {
            'path': '',
            'select': [
                'e',
            ],
            'enum_select': {},
        },
    },
}
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
        {
            'locus': hl.Locus(
                contig='chr1',
                position=2,
                reference_genome='GRCh38',
            ),
            'alleles': ['A', 'C'],
            'a': hl.Struct(d=3),
            'b': hl.Struct(e=4),
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
MOCK_A_DATASET_HT = hl.Table.parallelize(
    [
        {
            'locus': hl.Locus(
                contig='chr1',
                position=1,
                reference_genome='GRCh38',
            ),
            'alleles': ['A', 'C'],
            'a': hl.Struct(d=1),
        },
        {
            'locus': hl.Locus(
                contig='chr1',
                position=2,
                reference_genome='GRCh38',
            ),
            'alleles': ['A', 'C'],
            'a': hl.Struct(d=3),
        },
    ],
    hl.tstruct(
        locus=hl.tlocus('GRCh38'),
        alleles=hl.tarray(hl.tstr),
        a=hl.tstruct(d=hl.tint32),
    ),
    key=['locus', 'alleles'],
    globals=hl.Struct(
        path='a_path',
        version='a_version',
        enums=hl.Struct(),
    ),
)
MOCK_B_DATASET_HT = hl.Table.parallelize(
    [
        {
            'locus': hl.Locus(
                contig='chr1',
                position=1,
                reference_genome='GRCh38',
            ),
            'alleles': ['A', 'C'],
            'b': hl.Struct(e=5, f=1),
        },
        {
            'locus': hl.Locus(
                contig='chr1',
                position=3,
                reference_genome='GRCh38',
            ),
            'alleles': ['A', 'C'],
            'b': hl.Struct(e=7, f=2),
        },
    ],
    hl.tstruct(
        locus=hl.tlocus('GRCh38'),
        alleles=hl.tarray(hl.tstr),
        b=hl.tstruct(e=hl.tint32, f=hl.tint32),
    ),
    key=['locus', 'alleles'],
    globals=hl.Struct(
        path='b_new_path',
        version='b_new_version',
        enums=hl.Struct(
            enum_1=[
                'D',
                'F',
            ],
        ),
    ),
)
EXPECTED_JOINED_DATA = [
    hl.Struct(
        locus=hl.Locus(
            contig='chr1',
            position=1,
            reference_genome='GRCh38',
        ),
        alleles=['A', 'C'],
        a=hl.Struct(d=1),
        b=hl.Struct(e=5, f=1),
    ),
    hl.Struct(
        locus=hl.Locus(
            contig='chr1',
            position=2,
            reference_genome='GRCh38',
        ),
        alleles=['A', 'C'],
        a=hl.Struct(d=3),
        b=None,
    ),
    hl.Struct(
        locus=hl.Locus(
            contig='chr1',
            position=3,
            reference_genome='GRCh38',
        ),
        alleles=['A', 'C'],
        a=None,
        b=hl.Struct(e=7, f=2),
    ),
]
EXPECTED_GLOBALS = [
    hl.Struct(
        date='2023-04-19T16:43:39.361110-04:56',
        paths=hl.Struct(
            a='a_path',
            b='b_new_path',
        ),
        versions=hl.Struct(
            a='a_version',
            b='b_new_version',
        ),
        enums=hl.Struct(
            a=hl.Struct(),
            b=hl.Struct(
                enum_1=[
                    'D',
                    'F',
                ],
            ),
        ),
    ),
]

MOCK_DATETIME = datetime(
    2023,
    4,
    19,
    16,
    43,
    39,
    361110,
    tzinfo=pytz.timezone('US/Eastern'),
)

PATH_TO_FILE_UNDER_TEST = 'v03_pipeline.lib.reference_data.dataset_table_operations'


class ReferenceDataCombineTest(unittest.TestCase):
    def test_get_enum_select_fields(self):
        ht = hl.Table.parallelize(
            [
                {'variant': ['1', '2'], 'sv_type': 'a', 'sample_fix': '1'},
                {
                    'variant': ['1', '3', '2'],
                    'sv_type': 'b',
                    'sample_fix': '2',
                },
                {'variant': ['1', '3'], 'sv_type': 'c', 'sample_fix': '3'},
                {'variant': ['4'], 'sv_type': 'd', 'sample_fix': '4'},
            ],
            hl.tstruct(
                variant=hl.dtype('array<str>'),
                sv_type=hl.dtype('str'),
                sample_fix=hl.dtype('str'),
            ),
        )
        enum_select_fields = get_enum_select_fields(
            {
                'variant': ['1', '2', '3', '4'],
                'sv_type': ['a', 'b', 'c', 'd'],
            },
            ht,
        )
        mapped_ht = ht.transmute(**enum_select_fields)
        self.assertListEqual(
            mapped_ht.collect(),
            [
                hl.Struct(variant_ids=[0, 1], sv_type_id=0, sample_fix='1'),
                hl.Struct(variant_ids=[0, 2, 1], sv_type_id=1, sample_fix='2'),
                hl.Struct(variant_ids=[0, 2], sv_type_id=2, sample_fix='3'),
                hl.Struct(variant_ids=[3], sv_type_id=3, sample_fix='4'),
            ],
        )

        enum_select_fields = get_enum_select_fields(
            {'sv_type': ['d']},
            ht,
        )
        mapped_ht = ht.select(**enum_select_fields)
        self.assertRaises(Exception, mapped_ht.collect)

    @mock.patch.dict(
        f'{PATH_TO_FILE_UNDER_TEST}.CONFIG',
        {
            'mock_dbnsfp': {
                '38': {
                    'path': '',
                    'select': [
                        'fathmm_MKL_coding_pred',
                    ],
                    'custom_select': dbnsfp_custom_select,
                    'enum_select': {
                        'MutationTaster_pred': ['D', 'A', 'N', 'P'],
                        'fathmm_MKL_coding_pred': ['D', 'N'],
                    },
                },
            },
            'mock_dbnsfp_mito': {
                '38': {
                    'path': '',
                    'custom_select': dbnsfp_mito_custom_select,
                    'enum_select': {
                        'MutationTaster_pred': ['D', 'A', 'N', 'P'],
                    },
                    'filter': lambda ht: ht.locus.contig == 'chrM',
                },
            },
        },
    )
    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.hl.read_table')
    def test_dbnsfp_select_and_filter(self, mock_read_table):
        mock_read_table.return_value = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'REVEL_score': hl.missing(hl.tstr),
                    'SIFT_score': '.;0.082',
                    'Polyphen2_HVAR_score': '.;0.401',
                    'MutationTaster_pred': 'P',
                    'fathmm_MKL_coding_pred': 'N',
                },
                {
                    'locus': hl.Locus(
                        contig='chrM',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    'REVEL_score': '0.052',
                    'SIFT_score': '.;0.082',
                    'Polyphen2_HVAR_score': '.;0.401',
                    'MutationTaster_pred': 'P',
                    'fathmm_MKL_coding_pred': 'D',
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                REVEL_score=hl.tstr,
                SIFT_score=hl.tstr,
                Polyphen2_HVAR_score=hl.tstr,
                MutationTaster_pred=hl.tstr,
                fathmm_MKL_coding_pred=hl.tstr,
            ),
            key='locus',
        )
        ht = get_dataset_ht(
            'mock_dbnsfp',
            ReferenceGenome.GRCh38,
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    mock_dbnsfp=hl.Struct(
                        REVEL_score=None,
                        SIFT_score=hl.eval(hl.float32(0.082)),
                        Polyphen2_HVAR_score=hl.eval(hl.float32(0.401)),
                        MutationTaster_pred_id=3,
                        fathmm_MKL_coding_pred_id=1,
                    ),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    mock_dbnsfp=hl.Struct(
                        REVEL_score=hl.eval(hl.float32(0.052)),
                        SIFT_score=hl.eval(hl.float32(0.082)),
                        Polyphen2_HVAR_score=hl.eval(hl.float32(0.401)),
                        MutationTaster_pred_id=3,
                        fathmm_MKL_coding_pred_id=0,
                    ),
                ),
            ],
        )
        ht = get_dataset_ht(
            'mock_dbnsfp_mito',
            ReferenceGenome.GRCh38,
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=hl.Locus(
                        contig='chrM',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    mock_dbnsfp_mito=hl.Struct(
                        SIFT_score=hl.eval(hl.float32(0.0820000022649765)),
                        MutationTaster_pred_id=3,
                    ),
                ),
            ],
        )

    @mock.patch.dict(
        f'{PATH_TO_FILE_UNDER_TEST}.CONFIG',
        {
            'a': {
                '38': {
                    'path': 'gs://a.com',
                    'select': ['b'],
                    'version': '2.2.2',
                },
            },
        },
    )
    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.hl.read_table')
    def test_parse_version(self, mock_read_table):
        ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'b': 1,
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    'b': 2,
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                b=hl.tint32,
            ),
            key=['locus'],
            globals=hl.Struct(
                version='2.2.2',
            ),
        )
        mock_read_table.return_value = ht
        self.assertCountEqual(
            get_dataset_ht(
                'a',
                ReferenceGenome.GRCh38,
            ).globals.collect(),
            [
                hl.Struct(
                    path='gs://a.com',
                    version='2.2.2',
                    enums=hl.Struct(),
                ),
            ],
        )
        mock_read_table.return_value = ht.annotate_globals(version=hl.missing(hl.tstr))

        self.assertCountEqual(
            get_dataset_ht(
                'a',
                ReferenceGenome.GRCh38,
            ).globals.collect(),
            [
                hl.Struct(
                    path='gs://a.com',
                    version='2.2.2',
                    enums=hl.Struct(),
                ),
            ],
        )

        mock_read_table.return_value = ht.annotate_globals(version='1.2.3')
        ht = get_dataset_ht(
            'a',
            ReferenceGenome.GRCh38,
        )
        self.assertRaises(Exception, ht.globals.collect)

    @mock.patch.dict(f'{PATH_TO_FILE_UNDER_TEST}.CONFIG', MOCK_CONFIG)
    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.hl.read_table')
    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.get_dataset_ht')
    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.datetime', wraps=datetime)
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_existing_joined_hts(
        self,
        mock_reference_dataset_collection_datasets,
        mock_datetime,
        mock_get_dataset_ht,
        mock_read_table,
    ):
        mock_reference_dataset_collection_datasets.return_value = ['a', 'b']
        mock_datetime.now.return_value = MOCK_DATETIME
        mock_read_table.return_value = MOCK_JOINED_REFERENCE_DATA_HT
        mock_get_dataset_ht.return_value = MOCK_B_DATASET_HT
        ht = update_existing_joined_hts(
            'destination',
            'b',
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            ReferenceDatasetCollection.INTERVAL,
        )
        self.assertCountEqual(
            ht.collect(),
            EXPECTED_JOINED_DATA,
        )
        self.assertCountEqual(ht.globals.collect(), EXPECTED_GLOBALS)

    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.get_dataset_ht')
    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.datetime', wraps=datetime)
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_or_create_joined_ht_one_dataset(
        self,
        mock_reference_dataset_collection_datasets,
        mock_datetime,
        mock_get_dataset_ht,
    ):
        mock_reference_dataset_collection_datasets.return_value = ['a', 'b']
        mock_datetime.now.return_value = MOCK_DATETIME
        mock_get_dataset_ht.return_value = MOCK_B_DATASET_HT

        ht = update_or_create_joined_ht(
            ReferenceDatasetCollection.INTERVAL,
            DatasetType.SNV_INDEL,
            ReferenceGenome.GRCh38,
            dataset='b',
            joined_ht=MOCK_JOINED_REFERENCE_DATA_HT,
        )
        self.assertCountEqual(
            ht.collect(),
            EXPECTED_JOINED_DATA,
        )
        self.assertCountEqual(ht.globals.collect(), EXPECTED_GLOBALS)

    @mock.patch.dict(f'{PATH_TO_FILE_UNDER_TEST}.CONFIG', MOCK_CONFIG)
    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.get_dataset_ht')
    @mock.patch(f'{PATH_TO_FILE_UNDER_TEST}.datetime', wraps=datetime)
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_or_create_joined_ht_all_datasets(
        self,
        mock_reference_dataset_collection_datasets,
        mock_datetime,
        mock_get_dataset_ht,
    ):
        mock_reference_dataset_collection_datasets.return_value = ['a', 'b']
        mock_datetime.now.return_value = MOCK_DATETIME
        mock_get_dataset_ht.side_effect = [MOCK_A_DATASET_HT, MOCK_B_DATASET_HT]

        empty_ht = hl.Table.parallelize(
            [],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
            ),
            key=('locus', 'alleles'),
            globals=hl.Struct(
                paths=hl.Struct(),
                versions=hl.Struct(),
                enums=hl.Struct(),
            ),
        )

        ht = update_or_create_joined_ht(
            ReferenceDatasetCollection.COMBINED,
            DatasetType.SNV_INDEL,
            ReferenceGenome.GRCh38,
            dataset=None,
            joined_ht=empty_ht,
        )
        self.assertCountEqual(
            ht.collect(),
            EXPECTED_JOINED_DATA,
        )
        self.assertCountEqual(ht.globals.collect(), EXPECTED_GLOBALS)
