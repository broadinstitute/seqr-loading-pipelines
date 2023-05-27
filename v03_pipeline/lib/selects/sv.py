from __future__ import annotations

from typing import TYPE_CHECKING

# TODO: remove me once hail is used.
if TYPE_CHECKING:
    import hail as hl


def variant_id(mt: hl.MatrixTable):
    return mt.rsid
