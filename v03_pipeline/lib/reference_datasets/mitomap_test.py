import unittest
from unittest.mock import patch

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDataset

TEST_MITOMAP_CSV = 'v03_pipeline/var/test/reference_data/test_mitomap.csv'


class MitomapTest(unittest.TestCase):
    def test_mitomap(self):
        with patch.object(
            ReferenceDataset,
            'raw_dataset_path',
            return_value=TEST_MITOMAP_CSV,
        ):
            ht = ReferenceDataset.mitomap.get_ht(ReferenceGenome.GRCh38)
            self.assertEqual(
                ht.collect(),
                [
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chrM',
                            position=583,
                            reference_genome='GRCh38',
                        ),
                        alleles=['G', 'A'],
                        pathogenic=True,
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chrM',
                            position=591,
                            reference_genome='GRCh38',
                        ),
                        alleles=['C', 'T'],
                        pathogenic=True,
                    ),
                    hl.Struct(
                        locus=hl.Locus(
                            contig='chrM',
                            position=616,
                            reference_genome='GRCh38',
                        ),
                        alleles=['T', 'C'],
                        pathogenic=True,
                    ),
                ],
            )
