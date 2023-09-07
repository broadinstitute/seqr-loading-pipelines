from enum import Enum

import hail as hl
import matplotlib.pyplot as plt

from v03_pipeline.lib.model import ReferenceGenome

IMPUTE_SEX_ANNOTATIONS = [
    'is_female',
    'f_stat',
    'n_called',
    'expected_homs',
    'observed_homs',
    'sex',
]


class Ploidy(Enum):
    AMBIGUOUS = 'ambiguous_sex'
    ANEUPLOIDY = 'sex_aneuploidy'
    FEMALE = 'XX'
    MALE = 'XY'


def get_contig_cov(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    contig: str,
    call_rate_threshold: float,
    af_threshold: float = 0.01,
) -> hl.Table:
    """
    Calculate mean contig coverage.

    :param mt: MatrixTable containing samples with chrY variants
    :param reference_genome: ReferenceGenome, either GRCh37 or GRCh38
    :param contig: Chosen chromosome.
    :param call_rate_threshold: Minimum call rate threshold.
    :param af_threshold: Minimum allele frequency threshold. Default is 0.01
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
    if contig == hl.get_reference(reference_genome.value).x_contigs[0]:
        mt = mt.filter_rows(mt.locus.in_x_nonpar())
    if contig == hl.get_reference(reference_genome.value).y_contigs[0]:
        mt = mt.filter_rows(mt.locus.in_y_nonpar())

    # Filter to common SNVs above defined callrate (should only have one index in the array because the MT only contains biallelic variants)
    mt = mt.filter_rows(af_threshold < mt.AF)
    mt = hl.variant_qc(mt)
    mt = mt.filter_rows(mt.variant_qc.call_rate > call_rate_threshold)
    mt = mt.select_cols(**{f'{contig}_mean_dp': hl.agg.mean(mt.DP)})
    return mt.cols()


def run_hails_impute_sex(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    xy_fstat_threshold: float,
    xx_fstat_threshold: float,
    aaf_threshold: float,
) -> hl.Table:
    """
    Impute sex, annotate MatrixTable with results, and output a histogram of fstat values.

    :param MatrixTable mt: MatrixTable containing samples to be ascertained for sex
    :param reference_genome: ReferenceGenome, either GRCh37 or GRCh38
    :param xy_fstat_threshold: F-stat threshold above which a sample will be called XY.
    :param xx_fstat_threshold: F-stat threshold below which a sample will be called XX.
    :param aaf_threshold: Alternate allele frequency threshold for `hl.impute_sex`.
    :return: Table with imputed sex annotations
    """

    # Filter to the X chromosome and impute sex
    mt = hl.filter_intervals(
        mt,
        [
            hl.parse_locus_interval(
                hl.get_reference(reference_genome.value).x_contigs[0],
                reference_genome=reference_genome.value,
            ),
        ],
    )
    print(mt.show())
    ht = hl.impute_sex(
        mt.GT,
        aaf_threshold=aaf_threshold,
        male_threshold=xy_fstat_threshold,
        female_threshold=xx_fstat_threshold,
    )
    mt = mt.annotate_cols(**ht[mt.col_key])
    return mt.cols()


def generate_fstat_plot(
    ht: hl.Table,
    xy_fstat_threshold: float,
    xx_fstat_threshold: float,
):
    # Plot histogram of fstat values
    df = ht.to_pandas()
    plt.clf()
    plt.hist(df['f_stat'])
    plt.xlabel('Fstat')
    plt.ylabel('Frequency')
    plt.axvline(xy_fstat_threshold, color='blue', linestyle='dashed', linewidth=1)
    plt.axvline(xx_fstat_threshold, color='red', linestyle='dashed', linewidth=1)
    return plt


def call_sex(  # noqa: PLR0913
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    use_chrY_cov: bool = False,  # noqa: N803
    chrY_cov_threshold: float = 0.1,  # noqa: N803
    normalization_contig: str = 'chr20',
    xy_fstat_threshold: float = 0.75,
    xx_fstat_threshold: float = 0.5,
    aaf_threshold: float = 0.05,
    call_rate_threshold: float = 0.25,
) -> hl.Table:
    """
    Call sex for the samples in a given callset and export results file to the desired path.

    :param mt: MatrixTable containing samples with chrY variants
    :param reference_genome: ReferenceGenome, either GRCh37 or GRCh38
    :param use_chrY_cov: Set to True to calculate and use chrY coverage for sex inference.
        Will also compute and report chrX coverages.
        Default is False
    :param chrY_cov_threshold: Y coverage threshold used to infer sex aneuploidies.
        XY samples below and XX samples above this threshold will be inferred as having aneuploidies.
        Default is 0.1
    :param normalization_contig: Chosen chromosome for calculating normalized coverage. Default is "chr20"
    :param xy_fstat_threshold: F-stat threshold above which a sample will be called XY. Default is 0.75
    :param xx_fstat_threshold: F-stat threshold below which a sample will be called XX. Default is 0.5
    :param aaf_threshold: Alternate allele frequency threshold for `hl.impute_sex`. Default is 0.05
    :param call_rate_threshold: Minimum required call rate. Default is 0.25
    :return Table with imputed sex annotations an
    """

    valid_contigs = {*reference_genome.autosomes, *reference_genome.sex_chromosomes}
    if normalization_contig not in valid_contigs:
        msg = f'Contig: {normalization_contig} is invalid for this reference genome'
        raise ValueError(msg)

    # Filter to SNVs and biallelics
    # NB: We should already have filtered biallelics, but just
    mt = mt.filter_rows(hl.is_snp(mt.alleles[0], mt.alleles[1]))

    # Filter to PASS variants only (variants with empty or missing filter set)
    # TODO: Make this an optional argument before moving to gnomad_methods
    mt = mt.filter_rows(
        hl.is_missing(mt.filters) | (mt.filters.length() == 0),
        keep=True,
    )

    ht = run_hails_impute_sex(
        mt,
        reference_genome,
        xy_fstat_threshold,
        xx_fstat_threshold,
        aaf_threshold,
    )

    annotations = [
        *IMPUTE_SEX_ANNOTATIONS,
        f'{normalization_contig}_mean_dp',
    ]
    if not use_chrY_cov:
        ht = ht.annotate(
            sex=(
                hl.case()
                .when(hl.is_missing(ht.is_female), Ploidy.AMBIGUOUS.value)
                .when(ht.is_female, Ploidy.FEMALE.value)
                .default(Ploidy.MALE.value)
            ),
        )
        return ht.select(*annotations)

    annotations = [
        *IMPUTE_SEX_ANNOTATIONS,
        f'{normalization_contig}_mean_dp',
        'chrY_mean_dp',
        'normalized_y_coverage',
        'chrX_mean_dp',
        'normalized_x_coverage',
    ]
    for contig in [
        normalization_contig,
        hl.get_reference(reference_genome.value).y_contigs[0],
        hl.get_reference(reference_genome.value).x_contigs[0],
    ]:
        contig_ht = get_contig_cov(
            mt,
            reference_genome,
            contig,
            call_rate_threshold,
        )
        ht = ht.annotate(**contig_ht[ht.s])
    ht = ht.annotate(
        normalized_y_coverage=hl.or_missing(
            ht[f'{normalization_contig}_mean_dp'] > 0,
            ht.chrY_mean_dp / ht[f'{normalization_contig}_mean_dp'],
        ),
        normalized_x_coverage=hl.or_missing(
            ht[f'{normalization_contig}_mean_dp'] > 0,
            ht.chrX_mean_dp / ht[f'{normalization_contig}_mean_dp'],
        ),
    )
    ht = ht.annotate(
        sex=(
            hl.case()
            .when(hl.is_missing(ht.is_female), Ploidy.AMBIGUOUS.value)
            .when(
                (
                    (
                        (ht.is_female)
                        & hl.is_defined(ht.normalized_y_coverage)
                        & (ht.normalized_y_coverage > chrY_cov_threshold)
                    )
                    | (
                        (~ht.is_female)
                        & hl.is_defined(ht.normalized_y_coverage)
                        & (ht.normalized_y_coverage < chrY_cov_threshold)
                    )
                ),
                Ploidy.ANEUPLOIDY.value,
            )
            .when(ht.is_female, Ploidy.FEMALE.value)
            .default(Ploidy.MALE.value)
        ),
    )
    return ht.select(*annotations)
