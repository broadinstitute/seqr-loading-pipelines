import unittest
from unittest.mock import Mock, patch

import hail as hl

from v03_pipeline.lib.reference_data.clinvar import (
    parsed_and_mapped_clnsigconf,
    parsed_clnsig,
)


class ClinvarTest(unittest.TestCase):
    def test_parsed_clnsig(self):
        ht = hl.Table.parallelize(
            [
                {'info': hl.Struct(CLNSIG=['Pathogenic|Affects'])},
                {
                    'info': hl.Struct(
                        CLNSIG=[
                            'Pathogenic/Likely_pathogenic/Pathogenic',
                            '_low_penetrance',
                        ],
                    ),
                },
                {
                    'info': hl.Struct(
                        CLNSIG=[
                            'Likely_pathogenic/Pathogenic',
                            '_low_penetrance|association|protective',
                        ],
                    ),
                },
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

    def test_parsed_and_mapped_clnsigconf(self):
        ht = hl.Table.parallelize(
            [
                {'info': hl.Struct(CLNSIGCONF=hl.missing(hl.tarray(hl.tstr)))},
                {
                    'info': hl.Struct(
                        CLNSIGCONF=[
                            'Pathogenic(8)|Likely_pathogenic(2)|Pathogenic',
                            '_low_penetrance(1)|Uncertain_significance(1)',
                        ],
                    ),
                },
            ],
            hl.tstruct(info=hl.tstruct(CLNSIGCONF=hl.tarray(hl.tstr))),
        )
        self.assertListEqual(
            parsed_and_mapped_clnsigconf(ht).collect(),
            [
                None,
                [
                    hl.Struct(count=9, pathogenicity_id=0),
                    hl.Struct(count=2, pathogenicity_id=4),
                    hl.Struct(count=1, pathogenicity_id=11),
                ],
            ],
        )
    #
    # @patch('v03_pipeline.lib.reference_data.urllib.request.urlretrieve')
    # @patch('v03_pipeline.lib.reference_data.safely_move_to_gcs')
    # def test_download_and_import_latest_clinvar_vcf(self, safely_move_to_gcs, urlretrieve, tempdir):
    #     pass
