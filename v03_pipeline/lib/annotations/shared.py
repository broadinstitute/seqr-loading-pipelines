from __future__ import annotations

from typing import Any

import hail as hl

from hail_scripts.computed_fields import variant_id as expression_helpers

from v03_pipeline.lib.model import ReferenceGenome

AMINO_ACIDS = [
    'A',
    'C',
    'D',
    'E',
    'F',
    'G',
    'H',
    'I',
    'K',
    'L',
    'M',
    'N',
    'P',
    'Q',
    'R',
    'S',
    'T',
    'V',
    'W',
    'Y',
    'X',
    '*',
    'U',
]
AMINO_ACIDS_LOOKUP = hl.dict(hl.enumerate(AMINO_ACIDS, index_first=False))

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
    'vaultRNA/vault_RNA',
    'bidirectional_promoter_lncRNA',
]
BIOTYPE_LOOKUP = hl.dict(hl.enumerate(BIOTYPES, index_first=False))

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
CONSEQUENCE_TERMS_LOOKUP = hl.dict(hl.enumerate(CONSEQUENCE_TERMS, index_first=False))

PROTEIN_CODING_ID = BIOTYPE_LOOKUP['protein_coding']

OMIT_CONSEQUENCE_TERMS = hl.set(
    [
        'upstream_gene_variant',
        'downstream_gene_variant',
    ],
)

SELECTED_ANNOTATIONS = [
    'canonical',
    'codons',
    'gene_id',
    'hgvsc',
    'hgvsp',
    'transcript_id',
]


def sample_id(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return mt.s


def rg37_locus(
    ht: hl.Table,
    reference_genome: ReferenceGenome,
    liftover_ref_path: str,
    **_: Any,
) -> hl.Expression | None:
    if reference_genome == ReferenceGenome.GRCh37:
        return None
    rg37 = hl.get_reference(ReferenceGenome.GRCh37.value)
    rg38 = hl.get_reference(ReferenceGenome.GRCh38.value)
    if not rg38.has_liftover(rg37):
        rg38.add_liftover(liftover_ref_path, rg37)
    return hl.liftover(ht.locus, ReferenceGenome.GRCh37.value)


def xpos(ht: hl.Table, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_xpos(ht.locus)


def variant_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_variant_id(ht)


def sorted_transcript_consequences(ht: hl.Table, **_: Any) -> hl.Expression:
    result = hl.sorted(
        ht.vep.transcript_consequences.map(
            lambda c: c.select(
                *SELECTED_ANNOTATIONS,
                amino_acid_ids=c.amino_acids.split('/').map(
                    lambda a: AMINO_ACIDS_LOOKUP[a],
                ),
                biotype_id=BIOTYPE_LOOKUP[c.biotype],
                consequence_term_ids=(
                    c.consequence_terms.filter(
                        lambda t: ~OMIT_CONSEQUENCE_TERMS.contains(t),
                    ).map(lambda t: CONSEQUENCE_TERMS_LOOKUP[t])
                ),
                is_lof_nagnag=c.lof_flags == 'NAGNAG_SITE',
                lof_filters=hl.or_missing(
                    (c.lof == 'LC') & hl.is_defined(c.lof_filter),
                    c.lof_filter.split('&|,'),
                ),
            ),
        ).filter(lambda c: c.consequence_term_ids.size() > 0),
        lambda c: (
            hl.bind(
                lambda is_coding, is_most_severe, is_canonical: (
                    hl.cond(
                        is_coding,
                        hl.cond(
                            is_most_severe,
                            hl.cond(is_canonical, 1, 2),
                            hl.cond(is_canonical, 3, 4),
                        ),
                        hl.cond(
                            is_most_severe,
                            hl.cond(is_canonical, 5, 6),
                            hl.cond(is_canonical, 7, 8),
                        ),
                    )
                ),
                c.biotype_id == PROTEIN_CODING_ID,
                hl.set(c.consequence_term_ids).contains(
                    CONSEQUENCE_TERMS_LOOKUP[ht.vep.most_severe_consequence],
                ),
                hl.or_else(c.canonical, 0) == 1,
            )
        ),
    )
    return hl.zip_with_index(result).map(
        lambda csq_with_index: csq_with_index[1].annotate(
            transcript_rank=csq_with_index[0],
        ),
    )
