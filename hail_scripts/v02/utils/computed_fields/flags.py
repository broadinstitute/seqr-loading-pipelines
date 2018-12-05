import hail as hl


def get_expr_for_lc_lof_flag(sortedTranscriptConsequences):
    """Flag a variant if no LoF annotations are marked HC"""
    return hl.bind(
        lambda lof_annotations: (lof_annotations.size() > 0) & lof_annotations.all(lambda csq: csq.lof != "HC"),
        sortedTranscriptConsequences.filter(lambda csq: csq.lof != ""),
    )


def get_expr_for_genes_with_lc_lof_flag(sorted_transcript_consequences):
    """
    From a variant's sorted transcript consequences, get the set of gene IDs where the variant has at least one
    LoF consequence in that gene and all the variant's LoF consequences in that gene are not marked HC.
    """
    return hl.bind(
        lambda lof_consequences: hl.set(lof_consequences.map(lambda csq: csq.gene_id)).filter(
            lambda gene_id: hl.bind(
                lambda lof_consequences_in_gene: (lof_consequences_in_gene.size() > 0)
                & (lof_consequences_in_gene.all(lambda csq: csq.lof != "HC")),
                lof_consequences.filter(lambda csq: csq.gene_id == gene_id),
            )
        ),
        sorted_transcript_consequences.filter(lambda csq: csq.lof != ""),
    )


def get_expr_for_loftee_flag_flag(sortedTranscriptConsequences):
    """Flag a variant if all annotations have LOFTEE flags"""
    return hl.bind(
        lambda lof_annotations: (lof_annotations.size() > 0) & lof_annotations.all(lambda csq: csq.lof_flags != ""),
        sortedTranscriptConsequences.filter(lambda csq: csq.lof != ""),
    )


def get_expr_for_genes_with_loftee_flag_flag(sorted_transcript_consequences):
    """
    From a variant's sorted transcript consequences, get the set of gene IDs where the variant has at least one
    LoF consequence in that gene and all the variant's LoF consequences in that gene are flagged by LOFTEE.
    """
    return hl.bind(
        lambda lof_consequences: hl.set(lof_consequences.map(lambda csq: csq.gene_id)).filter(
            lambda gene_id: hl.bind(
                lambda lof_consequences_in_gene: (lof_consequences_in_gene.size() > 0)
                & (lof_consequences_in_gene.all(lambda csq: csq.lof_flags != "")),
                lof_consequences.filter(lambda csq: csq.gene_id == gene_id),
            )
        ),
        sorted_transcript_consequences.filter(lambda csq: csq.lof != ""),
    )
