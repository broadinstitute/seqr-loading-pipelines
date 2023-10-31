import hail as hl


def _replace_chr_prefix(contig: str):
    return contig.replace('^chr', '')


def _get_expr_for_contig(locus: hl.expr.LocusExpression) -> hl.expr.StringExpression:
    """Normalized contig name"""
    return _replace_chr_prefix(locus.contig)


def _get_expr_for_contig_number(
    locus: hl.expr.LocusExpression,
) -> hl.expr.Int32Expression:
    """Convert contig name to contig number"""
    return hl.bind(
        lambda contig: (
            hl.case()
            .when(contig == 'X', 23)
            .when(contig == 'Y', 24)
            .when(contig[0] == 'M', 25)
            .default(hl.int(contig))
        ),
        _get_expr_for_contig(locus),
    )


def get_expr_for_variant_id(table, max_length=None):
    """Expression for computing <chrom>-<pos>-<ref>-<alt>. Assumes alleles were split.

    Args:
        max_length: (optional) length at which to truncate the <chrom>-<pos>-<ref>-<alt> string

    Return:
        string: "<chrom>-<pos>-<ref>-<alt>"
    """
    contig = _get_expr_for_contig(table.locus)
    variant_id = (
        contig
        + '-'
        + hl.str(table.locus.position)
        + '-'
        + table.alleles[0]
        + '-'
        + table.alleles[1]
    )
    if max_length is not None:
        return variant_id[0:max_length]
    return variant_id


def get_expr_for_xpos(locus: hl.expr.LocusExpression) -> hl.expr.Int64Expression:
    """Genomic position represented as a single number = contig_number * 10**9 + position.
    This represents chrom:pos more compactly and allows for easier sorting.
    """
    contig_number = _get_expr_for_contig_number(locus)
    return hl.int64(contig_number) * 1_000_000_000 + locus.position
