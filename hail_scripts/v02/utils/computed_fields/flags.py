import hail as hl


def get_expr_for_lc_lof_flag(sortedTranscriptConsequences):
    """Flag a variant if no LoF annotations are marked HC"""
    return hl.bind(
        lambda lof_annotations: (lof_annotations.size() > 0) & lof_annotations.all(lambda csq: csq.lof != "HC"),
        sortedTranscriptConsequences.filter(lambda csq: csq.lof != ""),
    )


def get_expr_for_loftee_flag_flag(sortedTranscriptConsequences):
    """Flag a variant if all annotations have LOFTEE flags"""
    return hl.bind(
        lambda lof_annotations: (lof_annotations.size() > 0) & lof_annotations.all(lambda csq: csq.lof_flags != ""),
        sortedTranscriptConsequences.filter(lambda csq: csq.lof != ""),
    )
