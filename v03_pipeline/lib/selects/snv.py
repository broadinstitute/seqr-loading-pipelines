from typing import Any

import hail as hl

from hail_scripts.computed_fields import variant_id as expression_helpers


def original_alt_alleles(mt: hl.MatrixTable, **_: Any) -> hl.Expression:
    if not (hasattr(mt, 'locus_old') and hasattr(mt, 'alleles_old')):
        return hl.empty_array(hl.tstr)
    return expression_helpers.get_expr_for_variant_ids(mt.locus_old, mt.alleles_old)
