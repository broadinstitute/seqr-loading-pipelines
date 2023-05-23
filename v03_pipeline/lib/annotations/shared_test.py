import unittest

import hail as hl

from v03_pipeline.lib.annotations.custom import run_vep
from v03_pipeline.lib.annotations.shared import sorted_transcript_consequences
from v03_pipeline.lib.definitions import Env, ReferenceGenome


class SharedAnnotationsTest(unittest.TestCase):
    def test_sorted_transcript_consequences(self):
        ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='chr1',
                        position=871269,
                        reference_genome=ReferenceGenome.GRCh38.value,
                    ),
                    'alleles': ['A', 'C'],
                },
            ],
            hl.tstruct(
                locus=hl.tlocus(ReferenceGenome.GRCh38.value),
                alleles=hl.tarray(hl.tstr),
            ),
            key=['locus', 'alleles'],
        )
        mt = hl.MatrixTable.from_rows_table(ht)
        mt = run_vep(mt, Env.TEST, ReferenceGenome.GRCh38, None)
        mt = mt.annotate_rows(
            sorted_transcript_consequences=sorted_transcript_consequences(mt),
        )
        self.assertCountEqual(
            mt.sorted_transcript_consequences.collect(),
            [
                [
                    hl.Struct(
                        amino_acids='S/L',
                        biotype='protein_coding',
                        canonical=1,
                        codons='tCg/tTg',
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000327044.6:c.1667C>T',
                        hgvsp='ENSP00000317992.6:p.Ser556Leu',
                        lof=None,
                        lof_filter=None,
                        lof_flags=None,
                        lof_info='INTRON_END:881781,EXON_END:881925,EXON_START:881782,DE_NOVO_DONOR_MES:-7.36719797135343,DE_NOVO_DONOR_PROB:0.261170618766552,DE_NOVO_DONOR_POS:-138,INTRON_START:881667,DE_NOVO_DONOR_MES_POS:-138,MUTANT_DONOR_MES:4.93863747168278',
                        transcript_id='ENST00000327044',
                        consequence_term_ids=[11],
                        transcript_rank=0,
                    ),
                    hl.Struct(
                        amino_acids=None,
                        biotype='retained_intron',
                        canonical=None,
                        codons=None,
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000477976.1:n.3114C>T',
                        hgvsp=None,
                        lof=None,
                        lof_filter=None,
                        lof_flags=None,
                        lof_info=None,
                        transcript_id='ENST00000477976',
                        consequence_term_ids=[22, 26],
                        transcript_rank=1,
                    ),
                    hl.Struct(
                        amino_acids=None,
                        biotype='retained_intron',
                        canonical=None,
                        codons=None,
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000483767.1:n.523C>T',
                        hgvsp=None,
                        lof=None,
                        lof_filter=None,
                        lof_flags=None,
                        lof_info=None,
                        transcript_id='ENST00000483767',
                        consequence_term_ids=[22, 26],
                        transcript_rank=2,
                    ),
                ],
            ],
        )
