import unittest
from unittest.mock import patch

import hail as hl

from v03_pipeline.lib.model.definitions import ReferenceGenome
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset

EXAC_PATH = 'v03_pipeline/var/test/reference_data/exac_1.vcf'


class ExacTest(unittest.TestCase):
    def test_exac(self):
        with patch.object(
            ReferenceDataset,
            'raw_dataset_path',
            return_value=EXAC_PATH,
        ):
            ht = ReferenceDataset.exac.get_ht(ReferenceGenome.GRCh38)
            self.assertEqual(
                ht.collect(),
                [
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1', position=1046973, reference_genome='GRCh38'
                        ),
                        alleles=['G', 'A'],
                        AF_POPMAX=['NA', 'NFE'],
                        AF=1.702e-05,
                        AC_Adj=0,
                        AC_Het=0,
                        AC_Hom=0,
                        AC_Hemi=None,
                        AN_Adj=27700,
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chr1', position=1046973, reference_genome='GRCh38'
                        ),
                        alleles=['G', 'T'],
                        AF_POPMAX=['NA', 'NFE'],
                        AF=1.702e-05,
                        AC_Adj=1,
                        AC_Het=1,
                        AC_Hom=0,
                        AC_Hemi=None,
                        AN_Adj=27700,
                    ),
                ],
            )
