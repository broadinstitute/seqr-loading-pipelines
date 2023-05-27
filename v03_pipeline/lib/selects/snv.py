import hail as hl

from hail_scripts.computed_fields import variant_id as expression_helpers


def original_alt_alleles(mt: hl.MatrixTable, **_):
    # TODO: This assumes we annotate `locus_old` in this code because `split_multi_hts` drops the proper `old_locus`.
    # If we can get it to not drop it, we should revert this to `old_locus`
    return expression_helpers.get_expr_for_variant_ids(mt.locus_old, mt.alleles_old)
