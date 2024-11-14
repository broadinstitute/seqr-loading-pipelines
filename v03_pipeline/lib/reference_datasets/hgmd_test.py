import unittest
from unittest.mock import patch

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset

TEST_HGMD_VCF = 'v03_pipeline/var/test/reference_data/test_hgmd.vcf'


class HGMDTest(unittest.TestCase):
    def test_hgmd_38(self):
        with patch.object(
            ReferenceDataset,
            'raw_dataset_path',
            return_value=TEST_HGMD_VCF,
        ):
            ht = ReferenceDataset.hgmd.get_ht(ReferenceGenome.GRCh38)
            # self.assertEqual(
            #     ht.collect(),
            #     [
            #         hl.Struct(
            #             locus=hl.Locus(
            #                 contig='chr1',
            #                 position=10057,
            #                 reference_genome='GRCh38',
            #             ),
            #             alleles=['A', 'C'],
            #             AF=2.642333674884867e-05,
            #             AN=113536,
            #             AC=3,
            #             Hom=0,
            #             AF_POPMAX_OR_GLOBAL=3.779861071961932e-05,
            #             FAF_AF=7.019999884505523e-06,
            #             Hemi=0,
            #         ),
            #
            #     ],
            # )
