from __future__ import annotations

import io
from enum import Enum
from typing import TYPE_CHECKING

import hail as hl
import matplotlib.pyplot as plt

if TYPE_CHECKING:
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
    af_field: str,
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
    mt = hl.variant_qc(mt)
    mt.AF.show()
    mt = mt.filter_rows(
        (mt.variant_qc.call_rate > call_rate_threshold) & (mt[af_field] > af_threshold),
    )
    mt = mt.select_cols(**{f'{contig}_mean_dp': hl.agg.mean(mt.DP)})
    return mt.cols()


def generate_fstat_plot(
    ht: hl.Table,
    xy_fstat_threshold: float,
    xx_fstat_threshold: float,
) -> io.BytesIO:
    """
    # Plot histogram of fstat values
    # Returns the plot saved as a binary buffer.
    #
    # :params ht: the output hail table from call_sex
    # :param xy_fstat_threshold: F-stat threshold above which a sample will be called XY.
    # :param xx_fstat_threshold: F-stat threshold below which a sample will be called XX.
    """
    buf = io.BytesIO()
    df = ht.to_pandas()
    plt.clf()
    plt.hist(df['f_stat'])
    plt.xlabel('Fstat')
    plt.ylabel('Frequency')
    plt.axvline(xy_fstat_threshold, color='blue', linestyle='dashed', linewidth=1)
    plt.axvline(xx_fstat_threshold, color='red', linestyle='dashed', linewidth=1)
    plt.savefig(buf, format='png')
    buf.seek(0)
    return buf


def annotate_disrepant_sex(
    ht: hl.Table,
    pedigree_ht: hl.Table,
) -> hl.Table:
    """
    Adds annotations to the impute sex ht from the pedigree
    """
    ped_ht = pedigree_ht.key_by(s=pedigree_ht.Individual_ID).select('Sex')
    ped_ht = ped_ht.transmute(
        given_sex=hl.case()
        .when(ped_ht.Sex == 'M', Ploidy.MALE.value)
        .when(ped_ht.Sex == 'F', Ploidy.FEMALE.value)
        .default(ped_ht.Sex),
    )
    ht = ht.join(ped_ht, how='outer')
    return ht.annotate(discrepant_sex=ht.sex != ht.given_sex)


def call_sex(  # noqa: PLR0913
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    use_chrY_cov: bool = False,  # noqa: N803
    chrY_cov_threshold: float = 0.1,  # noqa: N803
    normalization_contig: str = 'chr20',
    xy_fstat_threshold: float = 0.75,
    xx_fstat_threshold: float = 0.5,
    aaf_threshold: float = 0.05,
    af_field: str = 'info.AF',
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
    :param af_field: Name of field containing allele frequency information. Default is 'info.AF'
    :param call_rate_threshold: Minimum required call rate. Default is 0.25
    :return Table with imputed sex annotations, and the fstat plot.
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

    impute_sex_ht = hl.impute_sex(
        mt.GT,
        male_threshold=xy_fstat_threshold,
        female_threshold=xx_fstat_threshold,
        aaf_threshold=aaf_threshold,
    )
    ht = mt.annotate_cols(**impute_sex_ht[mt.col_key]).cols()

    annotations = [
        *IMPUTE_SEX_ANNOTATIONS,
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
            af_field,
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
