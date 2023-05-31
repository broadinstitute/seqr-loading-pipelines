from __future__ import annotations

import datetime
from typing import Any

import hail as hl
import pytz

from hail_scripts.computed_fields import variant_id as expression_helpers


def xpos(ht: hl.Table, **_: Any) -> hl.Expression:
    # NB: `pos` is an aggregated field over samples.
    # I made the design choice to aggregate upstream so as to ensure
    # all of the annotation methods worked w/ hl.Table rather
    # than also support hl.MatrixTable.
    return expression_helpers.get_expr_for_xpos(
        hl.locus(
            expression_helpers.replace_chr_prefix(ht.chr),
            ht.pos,
        ),
    )


def variant_id(ht: hl.Table, **_: Any) -> hl.Expression:
    return hl.format(
        f'%s_%s_{datetime.datetime.now(tz=pytz.timezone("US/Eastern")).date():%m%d%Y}',
        ht.variant_name,
        ht.svtype,
    )
