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
               {'info': hl.struct(CLNSIG=['Pathogenic|Affects'])},
               {'info': hl.struct(CLNSIG=['Pathogenic/Likely_pathogenic/Pathogenic', '_low_penetrance'])},
               {'info': hl.struct(CLNSIG=['Likely_pathogenic/Pathogenic', '_low_penetrance|association|protective'])},
               {'info': hl.struct(CLNSIG=['Likely_pathogenic', '_low_penetrance'])},
               {'info': hl.struct(CLNSIG=['association|protective'])},
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
               {'info': hl.struct(CLNSIGCONF=hl.missing(hl.tarray(hl.tstr)))},
               {'info': hl.struct(CLNSIGCONF=['Pathogenic(8)|Likely_pathogenic(2)|Pathogenic', '_low_penetrance(1)|Uncertain_significance(1)'])},
            ],
            hl.tstruct(info=hl.tstruct(CLNSIGCONF=hl.tarray(hl.tstr))),
        )
        self.assertListEqual(
            parsed_clnsigconf(ht).collect(),
            [
                None,
                [
                    ('Likely_pathogenic', 2),
                    ('Pathogenic', 9),
                    ('Uncertain_significance', 1),
                ]
            ],
        )


