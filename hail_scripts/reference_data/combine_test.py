import unittest

import hail as hl

from hail_scripts.reference_data.combine import (
    get_enum_select_fields,
    update_joined_ht_globals,
)

class ReferenceDataCombineTest(unittest.TestCase):

    def test_get_enum_select_fields(self):
        ht = hl.Table.parallelize(
           [
               {'variant': ['1', '2'], 'sv_type': '2', 'sample_fix': '1', 'data' : 5},
               {'variant': ['1', '4'], 'sv_type': '2', 'sample_fix': '2', 'data' : 6},
               {'variant': ['2', '4', '4'], 'sv_type': '2', 'sample_fix': '3', 'data' : 7},
               {'variant': ['1', '3', '4'], 'sv_type': '2', 'sample_fix': '4', 'data' : 8},
           ],
           hl.tstruct(variant=hl.dtype('array<str>'), sv_type=hl.dtype('str'), sample_fix=hl.dtype('str'), data=hl.dtype('int32')),
        )
        enum_select_fields = get_enum_select_fields([
            {'src': 'variant', 'dst': 'variant_ids', 'mapping': hl.dict({'1': 1, '2': 2, '3': 3, '4': 4})},
            {'src': 'sv_type', 'dst': 'sv_type_id', 'mapping': hl.dict({'1': 1, '2': 2, '3': 3, '4': 4})},
        ], ht)
        mapped_ht = ht.select(**enum_select_fields)
        self.assertListEqual(
            mapped_ht.collect(),
            [
                hl.Struct(variant_ids=[1, 2], sv_type_id=2),
                hl.Struct(variant_ids=[1, 4], sv_type_id=2), 
                hl.Struct(variant_ids=[2, 4, 4], sv_type_id=2), 
                hl.Struct(variant_ids=[1, 3, 4], sv_type_id=2)
            ],
        )

        enum_select_fields = get_enum_select_fields([
            {'src': 'variant', 'dst': 'variant_ids', 'mapping': hl.dict({'1': 1})},
        ], ht)
        mapped_ht = ht.select(**enum_select_fields)
        self.assertRaises(Exception, mapped_ht.collect)

    def test_update_joined_ht_globals(self):
        ht = hl.Table.parallelize(
           [
               {'a': ['1', '2'], 'b': 2},
               {'a': ['1', '4'], 'b': 3},
           ],
           hl.tstruct(a=hl.tarray('str'), b=hl.tint32),
        )
        ht = update_joined_ht_globals(ht, ['cadd', 'screen'], '1.2.3', ['gnomad_exome_coverage'], '38')
        self.assertEqual(
            hl.Struct(
                date='2023-04-19T16:29:14.870116',
                datasets={
                    'cadd': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
                    'gnomad_exome_coverage': 'gs://seqr-reference-data/gnomad_coverage/GRCh38/exomes/gnomad.exomes.r2.1.coverage.liftover_grch38.ht',
                    'screen': 'gs://seqr-reference-data/GRCh38/ccREs/GRCh38-ccREs.ht'
                },
                version='1.2.3', 
                enum_definitions={
                    'screen': {
                        'regionType_ids': {
                            'CTCF-bound': 0,
                            'CTCF-only': 1,
                            'DNase-H3K4me3': 2,
                            'PLS': 3,
                            'dELS': 4,
                            'pELS': 5
                        }
                    }
                }
            )
        )






