import hail as hl


def get_expr_for_alt_allele(table:hl.Table) -> hl.str:
    return table.alleles[1]


def get_expr_for_contig(table:hl.Table) -> hl.str:
    """Normalized contig name"""
    return hl.str(table.locus.contig)


def get_expr_for_contig_number(table:hl.Table) -> hl.int:
    """Convert contig name to contig number"""
    return hl.bind(
        lambda contig: (
            hl.case().when(contig == "X", 23).when(contig == "Y", 24).when(contig[0] == "M", 25).default(hl.int(contig))
        ),
        get_expr_for_contig(table),
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


def get_expr_for_ref_allele(table):
    return table.alleles[0]


def get_expr_for_start_pos(table):
    return table.locus.position


def get_expr_for_variant_id(table, max_length=None):
    """Expression for computing <chrom>-<pos>-<ref>-<alt>. Assumes alleles were split.

    Args:
        max_length: (optional) length at which to truncate the <chrom>-<pos>-<ref>-<alt> string

    Return:
        string: "<chrom>-<pos>-<ref>-<alt>"
    """
    contig = get_expr_for_contig(table)
    variant_id = contig + "-" + hl.str(table.locus.position) + "-" + table.alleles[0] + "-" + table.alleles[1]
    if max_length is not None:
        return variant_id[0:max_length]
    return variant_id


def get_expr_for_xpos(table: hl.MatrixTable):
    """Genomic position represented as a single number = contig_number * 10**9 + position.
    This represents chrom:pos more compactly and allows for easier sorting.
    """
    contig_number = get_expr_for_contig_number(table)
    return hl.int64(contig_number) * 1_000_000_000 + table.locus.position
