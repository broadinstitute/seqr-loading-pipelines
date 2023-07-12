from typing import Any

import hail as hl


def GQ(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.if_else(is_called, mt.GQ, 0)


def AB(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.bind(
        lambda total: hl.if_else(
            (is_called) & (total != 0) & (hl.len(mt.AD) > 1),
            hl.float(mt.AD[1] / total),
            hl.missing(hl.tfloat),
        ),
        hl.sum(mt.AD),
    )


def DP(mt: hl.MatrixTable, **_: Any) -> hl.Expression:  # noqa: N802
    is_called = hl.is_defined(mt.GT)
    return hl.if_else(
        is_called & hl.is_defined(mt.AD),
        hl.int(hl.min(hl.sum(mt.AD), 32000)),
        hl.missing(hl.tint),
    )
