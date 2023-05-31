from __future__ import annotations

from typing import TYPE_CHECKING, Any

# TODO: remove me once hail is used.
if TYPE_CHECKING:
    import hail as hl


def variant_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return ht.rsid
