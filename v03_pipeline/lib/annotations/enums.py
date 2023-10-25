
import hail as hl
from v03_pipeline.lib.model import DatasetType


BIOTYPES = [
    'IG_C_gene',
    'IG_D_gene',
    'IG_J_gene',
    'IG_LV_gene',
    'IG_V_gene',
    'TR_C_gene',
    'TR_J_gene',
    'TR_V_gene',
    'TR_D_gene',
    'IG_pseudogene',
    'IG_C_pseudogene',
    'IG_J_pseudogene',
    'IG_V_pseudogene',
    'TR_V_pseudogene',
    'TR_J_pseudogene',
    'Mt_rRNA',
    'Mt_tRNA',
    'miRNA',
    'misc_RNA',
    'rRNA',
    'scRNA',
    'snRNA',
    'snoRNA',
    'ribozyme',
    'sRNA',
    'scaRNA',
    'lncRNA',
    'Mt_tRNA_pseudogene',
    'tRNA_pseudogene',
    'snoRNA_pseudogene',
    'snRNA_pseudogene',
    'scRNA_pseudogene',
    'rRNA_pseudogene',
    'misc_RNA_pseudogene',
    'miRNA_pseudogene',
    'TEC',
    'nonsense_mediated_decay',
    'non_stop_decay',
    'retained_intron',
    'protein_coding',
    'protein_coding_LoF',
    'protein_coding_CDS_not_defined',
    'processed_transcript',
    'non_coding',
    'ambiguous_orf',
    'sense_intronic',
    'sense_overlapping',
    'antisense/antisense_RNA',
    'antisense',
    'known_ncrna',
    'pseudogene',
    'processed_pseudogene',
    'polymorphic_pseudogene',
    'retrotransposed',
    'transcribed_processed_pseudogene',
    'transcribed_unprocessed_pseudogene',
    'transcribed_unitary_pseudogene',
    'translated_processed_pseudogene',
    'translated_unprocessed_pseudogene',
    'unitary_pseudogene',
    'unprocessed_pseudogene',
    'artifact',
    'lincRNA',
    'lincrna',
    'macro_lncRNA',
    '3prime_overlapping_ncRNA',
    'disrupted_domain',
    'vaultRNA/vault_RNA',
    'vaultRNA',
    'bidirectional_promoter_lncRNA',
]

CONSEQUENCE_TERMS = [
    'transcript_ablation',
    'splice_acceptor_variant',
    'splice_donor_variant',
    'stop_gained',
    'frameshift_variant',
    'stop_lost',
    'start_lost',  # new in v81
    'initiator_codon_variant',  # deprecated
    'transcript_amplification',
    'inframe_insertion',
    'inframe_deletion',
    'missense_variant',
    'protein_altering_variant',  # new in v79
    'splice_region_variant',
    'incomplete_terminal_codon_variant',
    'start_retained_variant',
    'stop_retained_variant',
    'synonymous_variant',
    'coding_sequence_variant',
    'mature_miRNA_variant',
    '5_prime_UTR_variant',
    '3_prime_UTR_variant',
    'non_coding_transcript_exon_variant',
    'non_coding_exon_variant',  # deprecated
    'intron_variant',
    'NMD_transcript_variant',
    'non_coding_transcript_variant',
    'nc_transcript_variant',  # deprecated
    'upstream_gene_variant',
    'downstream_gene_variant',
    'TFBS_ablation',
    'TFBS_amplification',
    'TF_binding_site_variant',
    'regulatory_region_ablation',
    'regulatory_region_amplification',
    'feature_elongation',
    'regulatory_region_variant',
    'feature_truncation',
    'intergenic_variant',
]

LOF_FILTERS = [
    'END_TRUNC',
    'INCOMPLETE_CDS',
    'EXON_INTRON_UNDEF',
    'SMALL_INTRON',
    'ANC_ALLELE',
    'NON_DONOR_DISRUPTING',
    'NON_ACCEPTOR_DISRUPTING',
    'RESCUE_DONOR',
    'RESCUE_ACCEPTOR',
    'GC_TO_GT_DONOR',
    '5UTR_SPLICE',
    '3UTR_SPLICE',
]

MITOTIP_PATHOGENICITIES = [
    'likely_pathogenic',
    'possibly_pathogenic',
    'possibly_benign',
    'likely_benign',
]

SV_TYPES = [
    'gCNV_DEL',
    'gCNV_DUP',
    'BND',
    'CPX',
    'CTX',
    'DEL',
    'DUP',
    'INS',
    'INV',
    'CNV',
]

SV_TYPE_DETAILS = [
    'INS_iDEL',
    'INVdel',
    'INVdup',
    'ME',
    'ME:ALU',
    'ME:LINE1',
    'ME:SVA',
    'dDUP',
    'dDUP_iDEL',
    'delINV',
    'delINVdel',
    'delINVdup',
    'dupINV',
    'dupINVdel',
    'dupINVdup',
]

SV_CONSEQUENCE_RANKS = [
    'LOF',
    'INTRAGENIC_EXON_DUP',
    'PARTIAL_EXON_DUP',
    'COPY_GAIN',
    'DUP_PARTIAL',
    'MSV_EXON_OVERLAP',
    'INV_SPAN',
    'UTR',
    'PROMOTER',
    'TSS_DUP',
    'BREAKEND_EXONIC',
    'INTRONIC',
    'NEAREST_TSS',
]


def annotate_enums(ht: hl.Table, dataset_type: DatasetType) -> hl.Table:
    formatting_annotation_names = {
        fa.__name__ for fa in dataset_type.formatting_annotation_fns
    }
    if 'sorted_transcript_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_transcript_consequences=hl.Struct(
                    biotype=BIOTYPES,
                    consequence_term=CONSEQUENCE_TERMS,
                    lof_filter=LOF_FILTERS,
                ),
            ),
        )
    if 'mitotip' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                mitotip=hl.Struct(
                    trna_prediction=MITOTIP_PATHOGENICITIES,
                ),
            ),
        )
    if 'sv_type_id' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sv_type=SV_TYPES,
            ),
        )
    if 'sv_type_detail_id' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(sv_type_detail=SV_TYPE_DETAILS),
        )
    if 'sorted_gene_consequences' in formatting_annotation_names:
        ht = ht.annotate_globals(
            enums=ht.enums.annotate(
                sorted_gene_consequences=hl.Struct(
                    major_consequence=SV_CONSEQUENCE_RANKS,
                ),
            ),
        )
    return ht
