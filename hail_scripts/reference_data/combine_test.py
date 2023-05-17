import unittest
from datetime import datetime
from unittest import mock

import hail as hl
import pytz

from hail_scripts.reference_data.combine import (
    get_enum_select_fields,
    get_ht,
    update_existing_joined_hts,
    update_joined_ht_globals,
)
from hail_scripts.reference_data.config import dbnsfp_custom_select


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
        'hail_scripts.reference_data.combine.CONFIG',
        {
            'mock_dbnsfp': {
                '38': {
                    'path': '',
                    'select': [
                        'fathmm_MKL_coding_pred',
                    ],
                    'custom_select': dbnsfp_custom_select,
                    'enum_select': {
                        'SIFT_pred': ['D', 'T'],
                        'Polyphen2_HVAR_pred': ['D', 'P', 'B'],
                        'MutationTaster_pred': ['D', 'A', 'N', 'P'],
                        'FATHMM_pred': ['D', 'T'],
                        'fathmm_MKL_coding_pred': ['D', 'N'],
                    },
                },
            },
        },
    )
    @mock.patch('hail_scripts.reference_data.combine.hl.read_table')
    def test_custom_select(self, mock_read_table):
        mock_read_table.return_value = hl.Table.parallelize(
            [
                {
                    'id': 0,
                    'REVEL_score': hl.missing(hl.tstr),
                    'SIFT_pred': '.;.;T',
                    'Polyphen2_HVAR_pred': '.;.;P',
                    'MutationTaster_pred': 'P',
                    'FATHMM_pred': '.;.;T',
                    'fathmm_MKL_coding_pred': 'N',
                },
                {
                    'id': 1,
                    'REVEL_score': '0.052',
                    'SIFT_pred': '.;.',
                    'Polyphen2_HVAR_pred': 'B',
                    'MutationTaster_pred': 'P',
                    'FATHMM_pred': '.;.;D',
                    'fathmm_MKL_coding_pred': 'D',
                },
            ],
            hl.tstruct(
                id=hl.tint32,
                REVEL_score=hl.tstr,
                SIFT_pred=hl.tstr,
                Polyphen2_HVAR_pred=hl.tstr,
                MutationTaster_pred=hl.tstr,
                FATHMM_pred=hl.tstr,
                fathmm_MKL_coding_pred=hl.tstr,
            ),
            key='id',
        )
        ht = get_ht('mock_dbnsfp', '38')
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    id=0,
                    mock_dbnsfp=hl.Struct(
                        REVEL_score=None,
                        SIFT_pred_id=1,
                        Polyphen2_HVAR_pred_id=1,
                        MutationTaster_pred_id=3,
                        FATHMM_pred_id=1,
                        fathmm_MKL_coding_pred_id=1,
                    ),
                ),
                hl.Struct(
                    id=1,
                    mock_dbnsfp=hl.Struct(
                        REVEL_score=0.052,
                        SIFT_pred_id=None,
                        Polyphen2_HVAR_pred_id=2,
                        MutationTaster_pred_id=3,
                        FATHMM_pred_id=0,
                        fathmm_MKL_coding_pred_id=0,
                    ),
                ),
            ],
        )

    @mock.patch('hail_scripts.reference_data.combine.datetime', wraps=datetime)
    def test_update_joined_ht_globals(self, mock_datetime):
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
        ht = hl.Table.parallelize(
            [
                {'a': ['1', '2'], 'b': 2},
                {'a': ['1', '4'], 'b': 3},
            ],
            hl.tstruct(a=hl.tarray('str'), b=hl.tint32),
        )
        ht = update_joined_ht_globals(
            ht,
            ['cadd', 'screen', 'gnomad_exome_coverage'],
            '1.2.3',
            '38',
        )
        self.assertEqual(
            ht.globals.collect()[0],
            hl.Struct(
                date='2023-04-19T16:43:39.361110-04:56',
                datasets={
                    'cadd': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
                    'gnomad_exome_coverage': 'gs://seqr-reference-data/gnomad_coverage/GRCh38/exomes/gnomad.exomes.r2.1.coverage.liftover_grch38.ht',
                    'screen': 'gs://seqr-reference-data/GRCh38/ccREs/GRCh38-ccREs.ht',
                },
                version='1.2.3',
                enum_definitions={
                    'screen': {
                        'region_type': [
                            'CTCF-bound',
                            'CTCF-only',
                            'DNase-H3K4me3',
                            'PLS',
                            'dELS',
                            'pELS',
                            'DNase-only',
                            'low-DNase',
                        ],
                    },
                },
            ),
        )

    @mock.patch.dict(
        'hail_scripts.reference_data.combine.CONFIG',
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
                },
            },
            'c_coverage': {
                '38': {
                    'path': '',
                    'select': [
                        'f',
                    ],
                },
            },
        },
    )
    @mock.patch('hail_scripts.reference_data.combine.hl.read_table')
    @mock.patch('hail_scripts.reference_data.combine.get_ht')
    def test_update_existing_joined_hts(self, mock_get_ht, mock_read_table):
        mock_read_table.return_value = hl.Table.parallelize(
            [
                {
                    'locus': 0,
                    'alleles': 10,
                    'a': hl.Struct(d=1),
                    'b': hl.Struct(e=2),
                    'c_coverage': hl.Struct(f=9),
                },
                {
                    'locus': 1,
                    'alleles': 10,
                    'a': hl.Struct(d=3),
                    'b': hl.Struct(e=4),
                    'c_coverage': None,
                },
            ],
            hl.tstruct(
                locus=hl.tint32,
                alleles=hl.tint32,
                a=hl.tstruct(d=hl.tint32),
                b=hl.tstruct(e=hl.tint32),
                c_coverage=hl.tstruct(f=hl.tint32),
            ),
            key=['locus', 'alleles'],
        )
        mock_get_ht.side_effect = [
            hl.Table.parallelize(
                [
                    {
                        'locus': 0,
                        'alleles': 10,
                        'b': hl.Struct(e=5),
                    },
                    {
                        'locus': 2,
                        'alleles': 10,
                        'b': hl.Struct(e=7),
                    },
                ],
                hl.tstruct(
                    locus=hl.tint32,
                    alleles=hl.tint32,
                    b=hl.tstruct(e=hl.tint32),
                ),
                key=['locus', 'alleles'],
            ),
            hl.Table.parallelize(
                [
                    {
                        'locus': 0,
                        'c_coverage': hl.Struct(f=12),
                    },
                    {
                        'locus': 1,
                        'c_coverage': hl.Struct(f=13),
                    },
                    {
                        'locus': 2,
                        'c_coverage': hl.Struct(f=14),
                    },
                ],
                hl.tstruct(
                    locus=hl.tint32,
                    c_coverage=hl.tstruct(f=hl.tint32),
                ),
                key=['locus'],
            ),
        ]
        ht = update_existing_joined_hts(
            'destination',
            '/tmp/checkpoint1.ht',
            'b',
            ['a', 'b', 'c_coverage'],
            '1.0.0',
            '38',
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=0,
                    alleles=10,
                    a=hl.Struct(d=1),
                    b=hl.Struct(e=5),
                    c_coverage=hl.Struct(f=9),
                ),
                hl.Struct(
                    locus=1,
                    alleles=10,
                    a=hl.Struct(d=3),
                    b=None,
                    c_coverage=None,
                ),
                hl.Struct(
                    locus=2,
                    alleles=10,
                    a=None,
                    b=hl.Struct(e=7),
                    c_coverage=None,
                ),
            ],
        )
        ht = update_existing_joined_hts(
            'destination',
            '/tmp/checkpoint2.ht',
            'c_coverage',
            ['a', 'b', 'c_coverage'],
            '1.0.0',
            '38',
        )
        self.assertCountEqual(
            ht.collect(),
            [
                hl.Struct(
                    locus=0,
                    alleles=10,
                    a=hl.Struct(d=1),
                    b=hl.Struct(e=2),
                    c_coverage=hl.Struct(f=12),
                ),
                hl.Struct(
                    locus=1,
                    alleles=10,
                    a=hl.Struct(d=3),
                    b=hl.Struct(e=4),
                    c_coverage=hl.Struct(f=13),
                ),
            ],
        )
