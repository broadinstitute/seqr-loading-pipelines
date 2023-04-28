from datetime import datetime
import unittest
from unittest import mock

import hail as hl

from hail_scripts.reference_data.combine import (
    get_enum_select_fields,
    update_joined_ht_globals,
)

class ReferenceDataCombineTest(unittest.TestCase):

    def test_get_enum_select_fields(self):
        ht = hl.Table.parallelize(
           [
               {'variant': ['1', '2'], 'sv_type': 'a', 'sample_fix': '1', 'data' : 5},
               {'variant': ['1', '3', '2'], 'sv_type': 'b', 'sample_fix': '2', 'data' : 6},
               {'variant': ['1', '3'], 'sv_type': 'c', 'sample_fix': '3', 'data' : 7},
               {'variant': ['4'], 'sv_type': 'd', 'sample_fix': '4', 'data' : 8},
           ],
           hl.tstruct(variant=hl.dtype('array<str>'), sv_type=hl.dtype('str'), sample_fix=hl.dtype('str'), data=hl.dtype('int32')),
        )
        enum_select_fields = get_enum_select_fields(
            {
                'variant': ['1', '2', '3', '4'],
                'sv_type': ['a', 'b', 'c', 'd'],
            },
        , ht)
        mapped_ht = ht.transmute(**enum_select_fields)
        self.assertListEqual(
            mapped_ht.collect(),
            [
                hl.Struct(variant_ids=[0, 1], sv_type_id=0),
                hl.Struct(variant_ids=[0, 2, 1], sv_type_id=1), 
                hl.Struct(variant_ids=[0, 2], sv_type_id=2), 
                hl.Struct(variant_ids=[3], sv_type_id=3)
            ],
        )

        enum_select_fields = get_enum_select_fields([
            {'sv_type': ['d']},
        ], ht)
        mapped_ht = ht.select(**enum_select_fields)
        self.assertRaises(Exception, mapped_ht.collect)

    @mock.patch('hail_scripts.reference_data.combine.datetime', wraps=datetime)
    def test_update_joined_ht_globals(self, mock_datetime):
        mock_datetime.now.return_value = datetime(2023, 4, 19, 16, 43, 39, 361110)
        ht = hl.Table.parallelize(
           [
               {'a': ['1', '2'], 'b': 2},
               {'a': ['1', '4'], 'b': 3},
           ],
           hl.tstruct(a=hl.tarray('str'), b=hl.tint32),
        )
        ht = update_joined_ht_globals(ht, ['cadd', 'screen'], '1.2.3', ['gnomad_exome_coverage'], '38')
        self.assertEqual(
            ht.globals.collect()[0],
            hl.Struct(
                date='2023-04-19T16:43:39.361110',
                datasets={
                    'cadd': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
                    'gnomad_exome_coverage': 'gs://seqr-reference-data/gnomad_coverage/GRCh38/exomes/gnomad.exomes.r2.1.coverage.liftover_grch38.ht',
                    'screen': 'gs://seqr-reference-data/GRCh38/ccREs/GRCh38-ccREs.ht'
                },
                version='1.2.3', 
                enum_definitions={
                    'screen': {
                        'regionType': [
                            'CTCF-bound',
                            'CTCF-only',
                            'DNase-H3K4me3',
                            'PLS',
                            'dELS',
                            'pELS',
                            'DNase-only',
                            'low-DNase',
                        ]
                    }
                }
            )
        )
