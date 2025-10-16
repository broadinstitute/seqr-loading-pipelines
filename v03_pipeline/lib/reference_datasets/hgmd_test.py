import unittest
from unittest.mock import patch

import hail as hl

from v03_pipeline.lib.core import ReferenceGenome
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset

TEST_HGMD_VCF = 'v03_pipeline/var/test/reference_datasets/raw/test_hgmd.vcf'


class HGMDTest(unittest.TestCase):
    def test_hgmd_38(self):
        with patch.object(
            ReferenceDataset,
            'path',
            return_value=TEST_HGMD_VCF,
        ):
            ht = ReferenceDataset.hgmd.get_ht(ReferenceGenome.GRCh38)
            self.assertEqual(
                ht.collect(),
                [
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1',
                            position=925942,
                            reference_genome='GRCh38',
                        ),
                        alleles=['A', 'G'],
                        accession='CM2039807',
                        class_id=1,
                    ),
                ],
            )
            self.assertEqual(
                ht.globals.collect()[0],
                hl.Struct(
                    version='1.0',
                    enums=hl.Struct(**ReferenceDataset.hgmd.enums),
                ),
            )
