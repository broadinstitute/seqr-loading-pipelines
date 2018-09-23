import hail as hl


def get_expr_for_alt_allele(table):
    return table.alleles[1]


def get_expr_for_contig(table):
    """Normalized contig name"""
    return hl.str(table.locus.contig)


def get_expr_for_contig_number(table):
    """Convert contig name to contig number"""
    return hl.bind(
        lambda contig: (
            hl.case().when(contig == "X", 23).when(contig == "Y", 24).when(contig[0] == "M", 25).default(hl.int(contig))
        ),
        get_expr_for_contig(table),
    )


def get_expr_for_original_alt_alleles_set(split_variant):
    """Compute variant id for each original alt allele in a variant split with hl.split_multi"""
    return hl.set(
        split_variant.old_alleles[1:].map(
            lambda alt: get_expr_for_variant_id(hl.min_rep(split_variant.old_locus, [split_variant.old_alleles[0], alt]))
        )
    )


def get_expr_for_ref_allele(table):
    return table.alleles[0]


def get_expr_for_start_pos(table):
    return table.locus.position


def get_expr_for_variant_id(table, max_length=None):
    """Expression for computing <chrom>-<pos>-<ref>-<alt>

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


def get_expr_for_xpos(table):
    """Genomic position represented as a single number = contig_number * 10**9 + position.
    This represents chrom:pos more compactly and allows for easier sorting.
    """
    contig_number = get_expr_for_contig_number(table)
    return hl.int64(contig_number) * 1_000_000_000 + table.locus.position
