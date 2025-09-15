import hail as hl

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
    'macro_lncRNA',
    '3prime_overlapping_ncRNA',
    'disrupted_domain',
    'vault_RNA',
    'bidirectional_promoter_lncRNA',
    '3prime_overlapping_ncrna',
]

REGULATORY_BIOTYPES = [
    'enhancer',
    'promoter',
    'CTCF_binding_site',
    'TF_binding_site',
    'open_chromatin_region',
]

TRANSCRIPT_CONSEQUENCE_TERMS = [
    'transcript_ablation',
    'splice_acceptor_variant',
    'splice_donor_variant',
    'stop_gained',
    'frameshift_variant',
    'stop_lost',
    'start_lost',
    'inframe_insertion',
    'inframe_deletion',
    'missense_variant',
    'protein_altering_variant',
    'splice_donor_5th_base_variant',
    'splice_region_variant',
    'splice_donor_region_variant',
    'splice_polypyrimidine_tract_variant',
    'incomplete_terminal_codon_variant',
    'start_retained_variant',
    'stop_retained_variant',
    'synonymous_variant',
    'coding_sequence_variant',
    'mature_miRNA_variant',
    '5_prime_UTR_variant',
    '3_prime_UTR_variant',
    'non_coding_transcript_exon_variant',
    'intron_variant',
    'NMD_transcript_variant',
    'non_coding_transcript_variant',
    'coding_transcript_variant',
    'upstream_gene_variant',
    'downstream_gene_variant',
    'intergenic_variant',
    'sequence_variant',
]

MOTIF_CONSEQUENCE_TERMS = [
    'TFBS_ablation',
    'TFBS_amplification',
    'TF_binding_site_variant',
    'TFBS_fusion',
    'TFBS_translocation',
]

REGULATORY_CONSEQUENCE_TERMS = [
    'regulatory_region_ablation',
    'regulatory_region_amplification',
    'regulatory_region_variant',
    'regulatory_region_fusion',
]

FIVEUTR_CONSEQUENCES = [
    '5_prime_UTR_premature_start_codon_gain_variant',  # uAUG_gained
    '5_prime_UTR_premature_start_codon_loss_variant',  # uAUG_lost
    '5_prime_UTR_stop_codon_gain_variant',  # uSTOP_gained
    '5_prime_UTR_stop_codon_loss_variant',  # uSTOP_lost
    '5_prime_UTR_uORF_frameshift_variant',  # uFrameshift
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

CLINVAR_ASSERTIONS = [
    'Affects',
    'association',
    'association_not_found',
    'confers_sensitivity',
    'drug_response',
    'low_penetrance',
    'not_provided',
    'other',
    'protective',
    'risk_factor',
    'no_classification_for_the_single_variant',
    'no_classifications_from_unflagged_records',
]

CLINVAR_DEFAULT_PATHOGENICITY = 'No_pathogenic_assertion'

# NB: sorted by pathogenicity
CLINVAR_PATHOGENICITIES = [
    'Pathogenic',
    'Pathogenic/Likely_pathogenic',
    'Pathogenic/Likely_pathogenic/Established_risk_allele',
    'Pathogenic/Likely_pathogenic/Likely_risk_allele',
    'Pathogenic/Likely_risk_allele',
    'Likely_pathogenic',
    'Likely_pathogenic/Likely_risk_allele',
    'Established_risk_allele',
    'Likely_risk_allele',
    'Conflicting_classifications_of_pathogenicity',
    'Uncertain_risk_allele',
    'Uncertain_significance/Uncertain_risk_allele',
    'Uncertain_significance',
    CLINVAR_DEFAULT_PATHOGENICITY,
    'Likely_benign',
    'Benign/Likely_benign',
    'Benign',
]


CLINVAR_PATHOGENICITIES_LOOKUP = hl.dict(
    hl.enumerate(CLINVAR_PATHOGENICITIES, index_first=False),
)
