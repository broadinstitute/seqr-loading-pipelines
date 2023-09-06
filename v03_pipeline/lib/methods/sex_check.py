import logging

from v03_pipeline.lib.model import ReferenceGenome


def get_contig_cov(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    contig: str,
    call_rate_threshold: float = 0.25,
    af_threshold: float = 0.01,
    af_field: str = 'AF',
) -> hl.Table:
    """
    Calculate mean chromosome coverage.

    :param mt: MatrixTable containing samples with chrY variants
    :param reference_genome: ReferenceGenome, either GRCh37 or GRCh38
    :param contig: Chosen chromosome.
    :param call_rate_threshold: Minimum call rate threshold. Default is 0.25
    :param af_threshold: Minimum allele frequency threshold. Default is 0.01
    :param af_field: Name of field containing allele frequency information. Default is "AF"
    :return: Table annotated with mean coverage of specified chromosome
    """

    valid_contigs = {*reference_genome.autosomes, *reference_genome.sex_chromosomes}
    if contig not in valid_contigs:
        msg = f'Contig: {contig} is invalid for this reference genome'
        raise ValueError(msg)

    sex_mt = hl.filter_intervals(
        mt,
        [hl.parse_locus_interval(contig, reference_genome=reference_genome.value)],
    )
    if 'X' in contig:
        sex_mt = sex_mt.filter_rows(sex_mt.locus.in_x_nonpar())
    if 'Y' in contig:
        sex_mt = sex_mt.filter_rows(sex_mt.locus.in_y_nonpar())

    # Filter to common SNVs above defined callrate (should only have one index in the array because the MT only contains biallelic variants)
    sex_mt = sex_mt.filter_rows(sex_mt[af_field] > af_threshold)
    sex_mt = hl.variant_qc(sex_mt)
    sex_mt = sex_mt.filter_rows(sex_mt.variant_qc.call_rate > call_rate_threshold)
    sex_mt = sex_mt.annotate_cols(**{f"{contig}_mean_dp": hl.agg.mean(sex_mt.DP)})
    return sex_mt.cols()