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
from v03_pipeline.lib.reference_data.combine import (
    get_enum_select_fields,
    get_ht,
    update_existing_joined_hts,
)
from v03_pipeline.lib.reference_data.config import (
    dbnsfp_custom_select,
    dbnsfp_mito_custom_select,
)


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
        'v03_pipeline.lib.reference_data.combine.CONFIG',
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
    @mock.patch('v03_pipeline.lib.reference_data.combine.hl.read_table')
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
        ht = get_ht(
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
        ht = get_ht(
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
        'v03_pipeline.lib.reference_data.combine.CONFIG',
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
    @mock.patch('v03_pipeline.lib.reference_data.combine.hl.read_table')
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
            get_ht(
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
            get_ht(
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
        ht = get_ht(
            'a',
            ReferenceGenome.GRCh38,
        )
        self.assertRaises(Exception, ht.globals.collect)

    @mock.patch.dict(
        'v03_pipeline.lib.reference_data.combine.CONFIG',
        {
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
        },
    )
    @mock.patch('v03_pipeline.lib.reference_data.combine.hl.read_table')
    @mock.patch('v03_pipeline.lib.reference_data.combine.get_ht')
    @mock.patch('v03_pipeline.lib.reference_data.combine.datetime', wraps=datetime)
    @mock.patch.object(ReferenceDatasetCollection, 'datasets')
    def test_update_existing_joined_hts(
        self,
        mock_reference_dataset_collection_datasets,
        mock_datetime,
        mock_get_ht,
        mock_read_table,
    ):
        mock_reference_dataset_collection_datasets.return_value = ['a', 'b']
        mock_datetime.now.return_value = datetime(
            2023,
            4,
            19,
            16,
            43,
            39,
            361110,
            tzinfo=pytz.timezone('US/Eastern'),
        )
        mock_read_table.return_value = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'alleles': 10,
                    'a': hl.Struct(d=1),
                    'b': hl.Struct(e=2),
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    'alleles': 10,
                    'a': hl.Struct(d=3),
                    'b': hl.Struct(e=4),
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tint32,
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
        mock_get_ht.return_value = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=1,
                        reference_genome='GRCh38',
                    ),
                    'alleles': 10,
                    'b': hl.Struct(e=5),
                },
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=3,
                        reference_genome='GRCh38',
                    ),
                    'alleles': 10,
                    'b': hl.Struct(e=7),
                },
            ],
            hl.tstruct(
                locus=hl.tlocus('GRCh38'),
                alleles=hl.tint32,
                b=hl.tstruct(e=hl.tint32),
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
        ht = update_existing_joined_hts(
            'destination',
            'b',
            ReferenceGenome.GRCh38,
            DatasetType.SNV_INDEL,
            ReferenceDatasetCollection.INTERVAL,
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
                    alleles=10,
                    a=hl.Struct(d=1),
                    b=hl.Struct(e=5),
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=2,
                        reference_genome='GRCh38',
                    ),
                    alleles=10,
                    a=hl.Struct(d=3),
                    b=None,
                ),
                hl.Struct(
                    locus=hl.Locus(
                        contig='chr1',
                        position=3,
                        reference_genome='GRCh38',
                    ),
                    alleles=10,
                    a=None,
                    b=hl.Struct(e=7),
                ),
            ],
        )
        self.assertCountEqual(
            ht.globals.collect(),
            [
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
            ],
        )
