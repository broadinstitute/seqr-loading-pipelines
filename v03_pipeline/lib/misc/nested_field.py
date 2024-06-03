import hail as hl


def parse_nested_field(t: hl.MatrixTable | hl.Table, fields: str):
    # Grab the field and continually select it from the hail table.
    expression = t
    for field in fields.split('.'):
        # Select from multi-allelic list.
        if field.endswith('#'):
            expression = expression[field[:-1]][
                (t.a_index if hasattr(t, 'a_index') else 1) - 1
            ]
        else:
            expression = expression[field]
    return expression
