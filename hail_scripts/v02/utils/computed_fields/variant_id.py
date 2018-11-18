import hail as hl
from typing import List

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


def get_expr_for_variant_ids(table, max_length=None) -> List[hl.str]:
    """Return a list of variant ids - one for each alt allele in the variant"""

    def compute_variant_id(alt):
        variant_id = get_expr_for_contig(table) + "-" + hl.str(table.locus.position) + "-" + table.alleles[0] + "-" + alt
        if max_length is not None:
            variant_id = variant_id[:max_length]
        return variant_id

    return table.alleles[1:].map(compute_variant_id)

def get_expr_for_orig_alt_alleles(table, max_length=None):
    """Return a list of alt-alleles. This can be used for saving original alleles before running hl.split_multi"""
    return hl.or_missing(hl.len(table.alleles) > 2, get_expr_for_variant_ids(table, max_length=max_length))

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
