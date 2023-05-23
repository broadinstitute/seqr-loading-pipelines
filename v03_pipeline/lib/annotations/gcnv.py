import hail as hl

from hail_scripts.computed_fields import variant_id


def start(
    mt: hl.MatrixTable,
    **kwargs,
):
    return hl.agg.min(mt.sample_start)


def contig(
    mt: hl.MatrixTable,
    **kwargs,
):
    return variant_id.replace_chr_prefix(mt.chr)


def pos(
    mt: hl.MatrixTable,
    **kwargs,
):
    return mt.start


def xpos(mt: hl.MatrixTable, **kwargs):
    return variant_id.get_expr_for_xpos(hl.locus(mt.contig, mt.pos))
