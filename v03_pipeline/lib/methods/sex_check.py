import hail as hl

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

    mt = hl.filter_intervals(
        mt,
        [hl.parse_locus_interval(contig, reference_genome=reference_genome.value)],
    )
    if 'X' in contig:
        mt = mt.filter_rows(mt.locus.in_x_nonpar())
    if 'Y' in contig:
        mt = mt.filter_rows(mt.locus.in_y_nonpar())

    # Filter to common SNVs above defined callrate (should only have one index in the array because the MT only contains biallelic variants)
    mt = mt.filter_rows(mt[af_field] > af_threshold)
    mt = hl.variant_qc(mt)
    mt = mt.filter_rows(mt.variant_qc.call_rate > call_rate_threshold)
    mt = mt.select_cols(**{f'{contig}_mean_dp': hl.agg.mean(mt.DP)})
    return mt.cols()
