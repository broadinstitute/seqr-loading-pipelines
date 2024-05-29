import unittest
from unittest.mock import Mock, patch

import hail as hl

from v03_pipeline.lib.annotations.vep import (
    sorted_motif_feature_consequences,
    sorted_regulatory_feature_consequences,
    sorted_transcript_consequences,
)
from v03_pipeline.lib.model import DatasetType, ReferenceGenome
from v03_pipeline.lib.vep import run_vep
from v03_pipeline.var.test.vep.mock_vep_data import MOCK_37_VEP_DATA, MOCK_38_VEP_DATA


class VepAnnotationsTest(unittest.TestCase):
    @patch('v03_pipeline.lib.vep.validate_vep_config_reference_genome')
    @patch('v03_pipeline.lib.vep.hl.vep')
    def test_sorted_transcript_consequences_37(
        self,
        mock_vep: Mock,
        mock_validate: Mock,
    ) -> None:
        ht = hl.Table.parallelize(
            [
                {
                    'locus': hl.Locus(
                        contig='1',
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
        mock_vep.return_value = ht.annotate(vep=MOCK_37_VEP_DATA)
        mock_validate.return_value = None
        ht = run_vep(
            ht,
            DatasetType.SNV_INDEL,
            ReferenceGenome.GRCh37,
        )
        ht = ht.select(
            sorted_transcript_consequences=sorted_transcript_consequences(
                ht,
                ReferenceGenome.GRCh37,
            ),
        )
        self.assertCountEqual(
            ht.sorted_transcript_consequences.collect(),
            [
                [
                    hl.Struct(
                        amino_acids='S/L',
                        canonical=1,
                        codons='tCg/tTg',
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000327044.6:c.1667C>T',
                        hgvsp='ENSP00000317992.6:p.Ser556Leu',
                        transcript_id='ENST00000327044',
                        biotype_id=39,
                        consequence_term_ids=[12],
                        is_lof_nagnag=None,
                        lof_filter_ids=[0, 1],
                        transcript_rank=0,
                    ),
                    hl.Struct(
                        amino_acids=None,
                        canonical=None,
                        codons=None,
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000477976.1:n.3114C>T',
                        hgvsp=None,
                        transcript_id='ENST00000477976',
                        biotype_id=38,
                        consequence_term_ids=[26, 29],
                        is_lof_nagnag=None,
                        lof_filter_ids=None,
                        transcript_rank=1,
                    ),
                    hl.Struct(
                        amino_acids=None,
                        canonical=None,
                        codons=None,
                        gene_id='ENSG00000188976',
                        hgvsc='ENST00000483767.1:n.523C>T',
                        hgvsp=None,
                        transcript_id='ENST00000483767',
                        biotype_id=38,
                        consequence_term_ids=[26, 29],
                        is_lof_nagnag=None,
                        lof_filter_ids=None,
                        transcript_rank=2,
                    ),
                ],
            ],
        )

    @patch('v03_pipeline.lib.vep.validate_vep_config_reference_genome')
    @patch('v03_pipeline.lib.vep.hl.vep')
    def test_sorted_transcript_consequences_38(
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
        mock_vep.return_value = ht.annotate(vep=MOCK_38_VEP_DATA)
        mock_validate.return_value = None
        ht = run_vep(
            ht,
            DatasetType.SNV_INDEL,
            ReferenceGenome.GRCh38,
        )
        ht = ht.select(
            sorted_transcript_consequences=sorted_transcript_consequences(
                ht,
                ReferenceGenome.GRCh38,
            ),
        )
        self.assertCountEqual(
            ht.sorted_transcript_consequences.collect()[0],
            [
                hl.Struct(
                    amino_acids='S/L',
                    canonical=1,
                    codons='tCg/tTg',
                    gene_id='ENSG00000188976',
                    hgvsc='ENST00000327044.6:c.1667C>T',
                    hgvsp='ENSP00000317992.6:p.Ser556Leu',
                    transcript_id='ENST00000327044',
                    mane_select='NM_015658.4',
                    mane_plus_clinical=None,
                    biotype_id=39,
                    consequence_term_ids=[12],
                    exon=hl.Struct(index=15, total=19),
                    intron=None,
                    alphamissense=hl.Struct(pathogenicity=0.10000000149011612),
                    loftee=hl.Struct(
                        is_lof_nagnag=None,
                        lof_filter_ids=[0, 1],
                    ),
                    spliceregion=hl.Struct(
                        extended_intronic_splice_region_variant=False,
                    ),
                    utrrannotator=hl.Struct(
                        existing_inframe_oorfs=None,
                        existing_outofframe_oorfs=None,
                        existing_uorfs=None,
                        fiveutr_consequence_id=None,
                        fiveutr_annotation=None,
                    ),
                    transcript_rank=0,
                ),
                hl.Struct(
                    amino_acids=None,
                    canonical=None,
                    codons=None,
                    gene_id='ENSG00000188976',
                    hgvsc='ENST00000477976.1:n.3114C>T',
                    hgvsp=None,
                    transcript_id='ENST00000477976',
                    mane_select=None,
                    mane_plus_clinical=None,
                    biotype_id=38,
                    consequence_term_ids=[26, 29],
                    exon=hl.Struct(index=13, total=17),
                    intron=None,
                    alphamissense=hl.Struct(pathogenicity=0.9700000286102295),
                    loftee=hl.Struct(
                        is_lof_nagnag=None,
                        lof_filter_ids=None,
                    ),
                    spliceregion=hl.Struct(
                        extended_intronic_splice_region_variant=False,
                    ),
                    utrrannotator=hl.Struct(
                        existing_inframe_oorfs=None,
                        existing_outofframe_oorfs=None,
                        existing_uorfs=None,
                        fiveutr_consequence_id=None,
                        fiveutr_annotation=None,
                    ),
                    transcript_rank=1,
                ),
                hl.Struct(
                    amino_acids=None,
                    canonical=None,
                    codons=None,
                    gene_id='ENSG00000188976',
                    hgvsc='ENST00000483767.1:n.523C>T',
                    hgvsp=None,
                    transcript_id='ENST00000483767',
                    mane_select=None,
                    mane_plus_clinical=None,
                    biotype_id=38,
                    consequence_term_ids=[26, 16, 29],
                    exon=hl.Struct(index=1, total=5),
                    intron=None,
                    alphamissense=hl.Struct(pathogenicity=None),
                    loftee=hl.Struct(
                        is_lof_nagnag=None,
                        lof_filter_ids=None,
                    ),
                    spliceregion=hl.Struct(
                        extended_intronic_splice_region_variant=True,
                    ),
                    utrrannotator=hl.Struct(
                        existing_inframe_oorfs=0,
                        existing_outofframe_oorfs=1,
                        existing_uorfs=0,
                        fiveutr_consequence_id=1,
                        fiveutr_annotation=hl.Struct(
                            type='OutOfFrame_oORF',
                            KozakContext='TTTATGC',
                            KozakStrength='Weak',
                            DistanceToCDS=40,
                            CapDistanceToStart=20,
                            DistanceToStop=75,
                            Evidence=False,
                            AltStop=None,
                            AltStopDistanceToCDS=None,
                            FrameWithCDS=None,
                            StartDistanceToCDS=None,
                            newSTOPDistanceToCDS=None,
                            alt_type=None,
                            alt_type_length=None,
                            ref_StartDistanceToCDS=None,
                            ref_type=None,
                            ref_type_length=None,
                        ),
                    ),
                    transcript_rank=2,
                ),
            ],
        )

    @patch('v03_pipeline.lib.vep.validate_vep_config_reference_genome')
    @patch('v03_pipeline.lib.vep.hl.vep')
    def test_sorted_other_feature_consequences(
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
        mock_vep.return_value = ht.annotate(vep=MOCK_38_VEP_DATA)
        mock_validate.return_value = None
        ht = run_vep(
            ht,
            DatasetType.SNV_INDEL,
            ReferenceGenome.GRCh38,
        )
        ht = ht.select(
            sorted_motif_feature_consequences=sorted_motif_feature_consequences(ht),
            sorted_regulatory_feature_consequences=sorted_regulatory_feature_consequences(
                ht,
            ),
        )
        self.assertCountEqual(
            ht.sorted_motif_feature_consequences.collect()[0],
            [None],
        )
        self.assertCountEqual(
            ht.sorted_regulatory_feature_consequences.collect()[0],
            [
                hl.Struct(
                    biotype_id=53,
                    consequence_term_ids=[0],
                    regulatory_feature_id='regulatory_2',
                    transcript_rank=0,
                ),
                hl.Struct(
                    biotype_id=53,
                    consequence_term_ids=[2],
                    regulatory_feature_id='regulatory_1',
                    transcript_rank=1,
                ),
            ],
        )
