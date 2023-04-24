import unittest
from unittest import mock

import hail as hl

from hail_scripts.utils.clinvar import (
    parsed_clnsig,
    parsed_clnsigconf,
)

class ClinvarTest(unittest.TestCase):
    def test_parsed_clnsig(self):
        ht = hl.Table.parallelize(
            [
               {'info': hl.Struct(CLNSIG=['Pathogenic|Affects'])},
               {'info': hl.Struct(CLNSIG=['Pathogenic/Likely_pathogenic/Pathogenic', '_low_penetrance'])},
               {'info': hl.Struct(CLNSIG=['Likely_pathogenic/Pathogenic', '_low_penetrance|association|protective'])},
               {'info': hl.Struct(CLNSIG=['Likely_pathogenic', '_low_penetrance'])},
               {'info': hl.Struct(CLNSIG=['association|protective'])},
            ],
            hl.tstruct(info=hl.tstruct(CLNSIG=hl.tarray(hl.tstr))),
        )
        self.assertListEqual(
            parsed_clnsig(ht).collect(),
            [
                ['Pathogenic', 'Affects'],
                ['Pathogenic/Likely_pathogenic', 'low_penetrance'],
                ['Likely_pathogenic', 'low_penetrance', 'association', 'protective'],
                ['Likely_pathogenic', 'low_penetrance'],
                ['association', 'protective'],
            ],
        )

    def test_parsed_clnsigconf(self):
        ht = hl.Table.parallelize(
            [
               {'info': hl.Struct(CLNSIGCONF=hl.missing(hl.tarray(hl.tstr)))},
               {'info': hl.Struct(CLNSIGCONF=['Pathogenic(8)|Likely_pathogenic(2)|Pathogenic', '_low_penetrance(1)|Uncertain_significance(1)'])},
            ],
            hl.tstruct(info=hl.tstruct(CLNSIGCONF=hl.tarray(hl.tstr))),
        )
        self.assertListEqual(
            parsed_clnsigconf(ht).collect(),
            [
                None,
                [
                    hl.Struct(count=2, pathogenicity_id=4),
                    hl.Struct(count=9, pathogenicity_id=0),
                    hl.Struct(count=1, pathogenicity_id=11),
                ]
            ],
        )


