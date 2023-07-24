from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    import hail as hl


def get_fields(
    t: hl.Table | hl.MatrixTable,
    fns: list[Callable[..., hl.Expression]],
    hts: dict[str, hl.Table] | None = None,
    **kwargs: Any,
) -> dict[str, hl.Expression]:
    if hts is None:
        hts = {}
    fields = {fn.__name__: fn(t, **hts, **kwargs) for fn in fns}
    return {k: v for k, v in fields.items() if v is not None}
