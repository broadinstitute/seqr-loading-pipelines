import hail as hl


def enum_map(field: hl.Expression, enum_values: list[str]) -> dict:
    lookup = hl.dict(
        hl.enumerate(enum_values, index_first=False).extend(
            # NB: adding missing values here allows us to
            # hard fail if a mapped key is present and has an unexpected value
            # but propagate missing values.
            [(hl.missing(hl.tstr), hl.missing(hl.tint32))],
        ),
    )
    if (
        isinstance(field.dtype, hl.tarray | hl.tset)
        and field.dtype.element_type == hl.tstr
    ):
        return field.map(
            lambda x: lookup[x],
        )
    return lookup[field]
