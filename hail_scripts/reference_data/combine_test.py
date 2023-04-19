import unittest

import hail as hl

from hail_scripts.reference_data.combine import get_enum_select_fields

class SeqrSVLoadingTest(unittest.TestCase):

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




