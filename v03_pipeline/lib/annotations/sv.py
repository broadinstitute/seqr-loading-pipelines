from __future__ import annotations

import hail as hl

def variant_id(
    mt: hl.MatrixTable,
    **kwargs
):
    return mt.rsid

def sorted_transcript_consequences(
    mt: hl.MatrixTable,
    **kwargs
):
    # TODO: implement me
    return None