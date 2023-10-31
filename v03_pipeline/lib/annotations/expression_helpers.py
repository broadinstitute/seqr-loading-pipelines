def _replace_chr_prefix(contig: str):
    return contig.replace("^chr", "")

def _get_expr_for_contig(locus: hl.expr.LocusExpression) -> hl.expr.StringExpression:
    """Normalized contig name"""
    return _replace_chr_prefix(locus.contig)

def _get_expr_for_contig_number(
    locus: hl.expr.LocusExpression
) -> hl.expr.Int32Expression:
    """Convert contig name to contig number"""
    return hl.bind(
        lambda contig: (
            hl.case()
            .when(contig == "X", 23)
            .when(contig == "Y", 24)
            .when(contig[0] == "M", 25)
            .default(hl.int(contig))
        ),
        _get_expr_for_contig(locus),
    )

def get_expr_for_variant_ids(
    locus: hl.expr.LocusExpression, alleles: hl.expr.ArrayExpression, max_length: int = None
) -> hl.expr.ArrayExpression:
    """Return a list of variant ids - one for each alt allele in the variant"""

    def compute_variant_id(alt):
        variant_id = locus.contig + "-" + hl.str(locus.position) + "-" + alleles[0] + "-" + alt
        if max_length is not None:
            variant_id = variant_id[:max_length]
        return variant_id

    return alleles[1:].map(compute_variant_id)

def get_expr_for_xpos(locus: hl.expr.LocusExpression) -> hl.expr.Int64Expression:
    """Genomic position represented as a single number = contig_number * 10**9 + position.
    This represents chrom:pos more compactly and allows for easier sorting.
    """
    contig_number = _get_expr_for_contig_number(locus)
    return hl.int64(contig_number) * 1_000_000_000 + locus.position
