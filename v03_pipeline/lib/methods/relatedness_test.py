import unittest

import hail as hl

from v03_pipeline.lib.methods.relatedness import annotate_families, call_relatedness
from v03_pipeline.lib.misc.io import import_pedigree
from v03_pipeline.lib.model import ReferenceGenome

TEST_PEDIGREE = 'v03_pipeline/var/test/pedigrees/test_pedigree_6.tsv'
TEST_SEX_AND_RELATEDNESS_CALLSET_MT = (
    'v03_pipeline/var/test/callsets/sex_and_relatedness_1.mt'
)


class RelatednessTest(unittest.TestCase):
    def test_call_relatedness(self):
        mt = hl.read_matrix_table(TEST_SEX_AND_RELATEDNESS_CALLSET_MT)
        ht = call_relatedness(
            mt,
            ReferenceGenome.GRCh38,
            af_field='AF',
            use_gnomad_in_ld_prune=False,
        )
        pedigree_ht = import_pedigree(TEST_PEDIGREE)
        ht = annotate_families(ht, pedigree_ht)
        self.assertCountEqual(
            ht.collect(),
            [],
        )
