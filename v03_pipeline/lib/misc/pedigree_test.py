import unittest

import hail as hl

from v03_pipeline.lib.methods.sex_check import Ploidy
from v03_pipeline.lib.misc.io import import_pedigree
from v03_pipeline.lib.misc.pedigree import (
    Family,
    SampleMeta,
    parse_pedigree_ht_to_families,
)

TEST_PEDIGREE_1 = 'v03_pipeline/var/test/pedigrees/test_pedigree_1.tsv'
TEST_PEDIGREE_2 = 'v03_pipeline/var/test/pedigrees/test_pedigree_2.tsv'


class PedigreesTest(unittest.TestCase):
    def test_empty_pedigree(self) -> None:
        with self.assertRaises(ValueError):
            _ = import_pedigree(TEST_PEDIGREE_1)

    def test_parse_lineage(self) -> None:
        #
        #
        #       sample_9   sample_10        sample_7
        #           \        /                  \                       |
        #   sample_6,sample_8      ---------     sample_3 ----- ?       |
        #                \                       /    \        /        |
        #                   sample_4, sample_5          sample_2        |    sample_1
        #
        #
        samples = Family.parse_direct_lineage(
            [
                hl.Struct(s='sample_1', maternal_s=None, paternal_s=None, sex='F'),
                hl.Struct(
                    s='sample_2',
                    maternal_s='sample_3',
                    paternal_s=None,
                    sex='M',
                ),
                hl.Struct(
                    s='sample_3',
                    maternal_s=None,
                    paternal_s='sample_7',
                    sex='F',
                ),
                hl.Struct(
                    s='sample_4',
                    maternal_s='sample_3',
                    paternal_s='sample_8',
                    sex='M',
                ),
                hl.Struct(
                    s='sample_5',
                    maternal_s='sample_3',
                    paternal_s='sample_8',
                    sex='M',
                ),
                hl.Struct(
                    s='sample_6',
                    maternal_s='sample_9',
                    paternal_s='sample_10',
                    sex='M',
                ),
                hl.Struct(s='sample_7', maternal_s=None, paternal_s=None, sex='M'),
                hl.Struct(
                    s='sample_8',
                    maternal_s='sample_9',
                    paternal_s='sample_10',
                    sex='F',
                ),
                hl.Struct(s='sample_9', maternal_s=None, paternal_s=None, sex='F'),
                hl.Struct(s='sample_10', maternal_s=None, paternal_s=None, sex='M'),
            ],
        )
        self.assertEqual(
            Family.parse_collateral_lineage(samples),
            {
                'sample_1': SampleMeta(
                    sex=Ploidy.FEMALE,
                    mother=None,
                    father=None,
                    maternal_grandmother=None,
                    maternal_grandfather=None,
                    paternal_grandmother=None,
                    paternal_grandfather=None,
                    siblings=[],
                    half_siblings=[],
                    aunt_uncles=[],
                ),
                'sample_2': SampleMeta(
                    sex=Ploidy.MALE,
                    mother='sample_3',
                    father=None,
                    maternal_grandmother=None,
                    maternal_grandfather='sample_7',
                    paternal_grandmother=None,
                    paternal_grandfather=None,
                    siblings=[],
                    half_siblings=['sample_4', 'sample_5'],
                    aunt_uncles=[],
                ),
                'sample_3': SampleMeta(
                    sex=Ploidy.FEMALE,
                    mother=None,
                    father='sample_7',
                    maternal_grandmother=None,
                    maternal_grandfather=None,
                    paternal_grandmother=None,
                    paternal_grandfather=None,
                    siblings=[],
                    half_siblings=[],
                    aunt_uncles=[],
                ),
                'sample_4': SampleMeta(
                    sex=Ploidy.MALE,
                    mother='sample_3',
                    father='sample_8',
                    maternal_grandmother=None,
                    maternal_grandfather='sample_7',
                    paternal_grandmother='sample_9',
                    paternal_grandfather='sample_10',
                    siblings=['sample_5'],
                    half_siblings=[],
                    aunt_uncles=['sample_6'],
                ),
                'sample_5': SampleMeta(
                    sex=Ploidy.MALE,
                    mother='sample_3',
                    father='sample_8',
                    maternal_grandmother=None,
                    maternal_grandfather='sample_7',
                    paternal_grandmother='sample_9',
                    paternal_grandfather='sample_10',
                    siblings=[],
                    half_siblings=[],
                    aunt_uncles=['sample_6'],
                ),
                'sample_6': SampleMeta(
                    sex=Ploidy.MALE,
                    mother='sample_9',
                    father='sample_10',
                    maternal_grandmother=None,
                    maternal_grandfather=None,
                    paternal_grandmother=None,
                    paternal_grandfather=None,
                    siblings=['sample_8'],
                    half_siblings=[],
                    aunt_uncles=[],
                ),
                'sample_7': SampleMeta(
                    sex=Ploidy.MALE,
                    mother=None,
                    father=None,
                    maternal_grandmother=None,
                    maternal_grandfather=None,
                    paternal_grandmother=None,
                    paternal_grandfather=None,
                    siblings=[],
                    half_siblings=[],
                    aunt_uncles=[],
                ),
                'sample_8': SampleMeta(
                    sex=Ploidy.FEMALE,
                    mother='sample_9',
                    father='sample_10',
                    maternal_grandmother=None,
                    maternal_grandfather=None,
                    paternal_grandmother=None,
                    paternal_grandfather=None,
                    siblings=[],
                    half_siblings=[],
                    aunt_uncles=[],
                ),
                'sample_9': SampleMeta(
                    sex=Ploidy.FEMALE,
                    mother=None,
                    father=None,
                    maternal_grandmother=None,
                    maternal_grandfather=None,
                    paternal_grandmother=None,
                    paternal_grandfather=None,
                    siblings=[],
                    half_siblings=[],
                    aunt_uncles=[],
                ),
                'sample_10': SampleMeta(
                    sex=Ploidy.MALE,
                    mother=None,
                    father=None,
                    maternal_grandmother=None,
                    maternal_grandfather=None,
                    paternal_grandmother=None,
                    paternal_grandfather=None,
                    siblings=[],
                    half_siblings=[],
                    aunt_uncles=[],
                ),
            },
        )

    def test_parse_project(self) -> None:
        pedigree_ht = import_pedigree(TEST_PEDIGREE_2)
        self.assertListEqual(
            parse_pedigree_ht_to_families(pedigree_ht),
            [
                Family(
                    family_guid='BBL_BC1-000345_1',
                    sample_lineage={
                        'BBL_BC1-000345_01_D1': SampleMeta(
                            sex=Ploidy.FEMALE,
                            mother='BBL_BC1-000345_03_D1',
                            father='BBL_BC1-000345_02_D1',
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=[],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                        'BBL_BC1-000345_02_D1': SampleMeta(
                            sex=Ploidy.MALE,
                            mother=None,
                            father=None,
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=[],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                        'BBL_BC1-000345_03_D1': SampleMeta(
                            sex=Ploidy.FEMALE,
                            mother=None,
                            father=None,
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=[],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                    },
                ),
                Family(
                    family_guid='BBL_HT-007-5195_1',
                    sample_lineage={
                        'BBL_HT-007-5195_01_D1': SampleMeta(
                            sex=Ploidy.FEMALE,
                            mother='BBL_HT-007-5195_03_D1',
                            father='BBL_HT-007-5195_02_D1',
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=[
                                'BBL_HT-007-5195_04_D1',
                                'BBL_HT-007-5195_05_D1',
                                'BBL_HT-007-5195_06_D1',
                            ],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                        'BBL_HT-007-5195_02_D1': SampleMeta(
                            sex=Ploidy.MALE,
                            mother=None,
                            father=None,
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=[],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                        'BBL_HT-007-5195_03_D1': SampleMeta(
                            sex=Ploidy.FEMALE,
                            mother=None,
                            father=None,
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=[],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                        'BBL_HT-007-5195_04_D1': SampleMeta(
                            sex=Ploidy.MALE,
                            mother='BBL_HT-007-5195_03_D1',
                            father='BBL_HT-007-5195_02_D1',
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=['BBL_HT-007-5195_05_D1', 'BBL_HT-007-5195_06_D1'],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                        'BBL_HT-007-5195_05_D1': SampleMeta(
                            sex=Ploidy.FEMALE,
                            mother='BBL_HT-007-5195_03_D1',
                            father='BBL_HT-007-5195_02_D1',
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=['BBL_HT-007-5195_06_D1'],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                        'BBL_HT-007-5195_06_D1': SampleMeta(
                            sex=Ploidy.MALE,
                            mother='BBL_HT-007-5195_03_D1',
                            father='BBL_HT-007-5195_02_D1',
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=[],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                    },
                ),
                Family(
                    family_guid='BBL_SDS1-000178_1',
                    sample_lineage={
                        'BBL_SDS1-000178_01_D1': SampleMeta(
                            sex=Ploidy.FEMALE,
                            mother=None,
                            father=None,
                            maternal_grandmother=None,
                            maternal_grandfather=None,
                            paternal_grandmother=None,
                            paternal_grandfather=None,
                            siblings=[],
                            half_siblings=[],
                            aunt_uncles=[],
                        ),
                    },
                ),
            ],
        )
