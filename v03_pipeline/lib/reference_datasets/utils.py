import hail as hl

from v03_pipeline.lib.misc.nested_field import parse_nested_field


def select_from_dict(selects: dict, ht: hl.Table) -> dict:
    select_fields = {}
    for key, val in selects.items():
        expression = parse_nested_field(ht, val)
        # Parse float64s into float32s to save space!
        if expression.dtype == hl.tfloat64:
            expression = hl.float32(expression)
        select_fields[key] = expression
    return select_fields
