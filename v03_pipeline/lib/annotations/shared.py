from __future__ import annotations

from typing import Any

import hail as hl

from hail_scripts.computed_fields import variant_id as expression_helpers

from v03_pipeline.lib.model import ReferenceGenome

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

OMIT_CONSEQUENCE_TERMS = hl.set(
    [
        'upstream_gene_variant',
        'downstream_gene_variant',
    ],
)

SELECTED_ANNOTATIONS = [
    'amino_acids',
    'biotype',
    'canonical',
    'codons',
    'gene_id',
    'hgvsc',
    'hgvsp',
    'lof',
    'lof_filter',
    'lof_flags',
    'lof_info',
    'transcript_id',
]


def rg37_locus(
    mt: hl.MatrixTable,
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
    return hl.liftover(mt.locus, ReferenceGenome.GRCh37.value)


def xpos(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_xpos(mt.locus)


def variant_id(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    return expression_helpers.get_expr_for_variant_id(mt)


def sorted_transcript_consequences(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    result = hl.sorted(
        mt.vep.transcript_consequences.map(
            lambda c: c.select(
                *SELECTED_ANNOTATIONS,
                consequence_term_ids=(
                    c.consequence_terms.filter(
                        lambda t: ~OMIT_CONSEQUENCE_TERMS.contains(t),
                    ).map(lambda t: CONSEQUENCE_TERMS_LOOKUP[t])
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
                hl.or_else(c.biotype, '') == 'protein_coding',
                hl.set(c.consequence_term_ids).contains(
                    CONSEQUENCE_TERMS_LOOKUP[mt.vep.most_severe_consequence],
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
