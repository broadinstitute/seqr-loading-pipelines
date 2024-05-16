import unittest
from unittest.mock import Mock, patch

import hail as hl

from v03_pipeline.lib.annotations.vep import sorted_transcript_consequences
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.vep import run_vep
from v03_pipeline.var.test.vep.mock_vep_data import MOCK_VEP_DATA


class VepAnnotationsTest(unittest.TestCase):
    @patch('v03_pipeline.lib.vep.validate_vep_config_reference_genome')
    @patch('v03_pipeline.lib.vep.hl.vep')
    def test_sorted_transcript_consequences(
        self,
        mock_vep: Mock,
        mock_validate: Mock,
    ) -> None:
        ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome=ReferenceGenome.GRCh37.value,
                    ),
                    'alleles': ['A', 'C'],
                },
            ],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh37.value),
                alleles=hl.tarray(hl.tstr),
            ),
            key=['locus', 'alleles'],
        )
        mock_vep.return_value = ht.annotate(vep=MOCK_VEP_DATA)
        mock_validate.return_value = None
        ht = run_vep(
            ht,
            DatasetType.SNV_INDEL,
            ReferenceGenome.GRCh37,
        )
        ht = ht.select(
            sorted_transcript_consequences=sorted_transcript_consequences(
                ht, ReferenceGenome.GRCh37,
            ),
        )
        self.assertCountEqual(
            ht.sorted_transcript_consequences.collect(),
            [
                [
                    hl.Struct(
                        amino_acids='S/L',
                        biotype_id=39,
                        canonical=1,
                        codons='tCg/tTg',
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000327044.6:c.1667C>T',
                        hgvsp='ENSP00000317992.6:p.Ser556Leu',
                        is_lof_nagnag=None,
                        lof_filter_ids=[0, 1],
                        transcript_id='ENST00000327044',
                        consequence_term_ids=[11],
                        transcript_rank=0,
                    ),
                    hl.Struct(
                        amino_acids=None,
                        biotype_id=38,
                        canonical=None,
                        codons=None,
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000477976.1:n.3114C>T',
                        hgvsp=None,
                        is_lof_nagnag=None,
                        lof_filter_ids=None,
                        transcript_id='ENST00000477976',
                        consequence_term_ids=[22, 26],
                        transcript_rank=1,
                    ),
                    hl.Struct(
                        amino_acids=None,
                        biotype_id=38,
                        canonical=None,
                        codons=None,
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000483767.1:n.523C>T',
                        hgvsp=None,
                        is_lof_nagnag=None,
                        lof_filter_ids=None,
                        transcript_id='ENST00000483767',
                        consequence_term_ids=[22, 26],
                        transcript_rank=2,
                    ),
                ],
            ],
        )
