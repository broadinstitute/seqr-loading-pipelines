import unittest

import hail as hl

from v03_pipeline.lib.misc.io import import_callset, select_relevant_fields
from v03_pipeline.lib.misc.pedigree import Family, Sample
from v03_pipeline.lib.misc.sample_ids import subset_samples
from v03_pipeline.lib.misc.male_non_par import (
    overwrite_male_non_par_calls,
)
from v03_pipeline.lib.model import DatasetType, ReferenceGenome, Sex

TEST_SV_VCF = 'v03_pipeline/var/test/callsets/sv_1.vcf'


class MaleNonParTest(unittest.TestCase):
    def test_overwrite_male_non_par_calls(self) -> None:
        mt = import_callset(TEST_SV_VCF, ReferenceGenome.GRCh38, DatasetType.SV)
        mt = select_relevant_fields(
            mt,
            DatasetType.SV,
        )
        mt = subset_samples(
            mt,
            hl.Table.parallelize(
                [{'s': sample_id} for sample_id in ['RGP_164_1', 'RGP_164_2']],
                hl.tstruct(s=hl.dtype('str')),
                key='s',
            ),
        )
        mt = overwrite_male_non_par_calls(
            mt,
            DatasetType.SV,
            {
                Family(
                    family_guid='family_1',
                    samples={
                        'RGP_164_1': Sample(sample_id='RGP_164_1', sex=Sex.FEMALE),
                        'RGP_164_2': Sample(sample_id='RGP_164_2', sex=Sex.MALE),
                    },
                ),
            },
        )
        mt = mt.filter_rows(mt.locus.contig == 'chrX')
        self.assertEqual(
            [
                hl.Locus(contig='chrX', position=3, reference_genome='GRCh38'),
                hl.Locus(contig='chrX', position=2781700, reference_genome='GRCh38'),
            ],
            mt.locus.collect(),
        )
        self.assertEqual(
            [
                hl.Call(alleles=[0, 0], phased=False),
                # END of this variant < start of the non-par region.
                hl.Call(alleles=[0, 1], phased=False),
                hl.Call(alleles=[0, 0], phased=False),
                hl.Call(alleles=[1], phased=False),
            ],
            mt.GT.collect(),
        )
        self.assertFalse(
            hasattr(mt, 'start_locus'),
        )
