import hail as hl


def get_expr_for_alt_allele(table:hl.Table) -> hl.str:
    return table.alleles[1]


def get_expr_for_contig(locus: hl.expr.LocusExpression) -> hl.expr.StringExpression:
    """Normalized contig name"""
    return locus.contig.replace("^chr", "")


def get_expr_for_contig_number(
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
        get_expr_for_contig(locus),
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


def get_expr_for_variant_type(table:hl.Table) -> hl.str:
    return hl.bind(
        lambda ref_len, alt_len: (
            hl.case()
                .when(ref_len > alt_len, "D")
                .when(ref_len < alt_len, "I")
                .when(ref_len > 1, "M")
                .default("S")
        ),
        hl.len(get_expr_for_ref_allele(table)),
        hl.len(get_expr_for_alt_allele(table)),
    )


def get_expr_for_ref_allele(table):
    return table.alleles[0]


def get_expr_for_start_pos(table):
    return table.locus.position


def get_expr_for_end_pos(table):
    return table.locus.position + hl.len(get_expr_for_ref_allele(table)) - 1


def get_expr_for_variant_id(table, max_length=None):
    """Expression for computing <chrom>-<pos>-<ref>-<alt>. Assumes alleles were split.

    Args:
        max_length: (optional) length at which to truncate the <chrom>-<pos>-<ref>-<alt> string

    Return:
        string: "<chrom>-<pos>-<ref>-<alt>"
    """
    contig = get_expr_for_contig(table.locus)
    variant_id = contig + "-" + hl.str(table.locus.position) + "-" + table.alleles[0] + "-" + table.alleles[1]
    if max_length is not None:
        return variant_id[0:max_length]
    return variant_id


def get_expr_for_xpos(locus: hl.expr.LocusExpression) -> hl.expr.Int64Expression:
    """Genomic position represented as a single number = contig_number * 10**9 + position.
    This represents chrom:pos more compactly and allows for easier sorting.
    """
    contig_number = get_expr_for_contig_number(locus)
    return hl.int64(contig_number) * 1_000_000_000 + locus.position
