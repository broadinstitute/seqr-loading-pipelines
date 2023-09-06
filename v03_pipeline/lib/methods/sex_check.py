import hail as hl
import matplotlib.pyplot as plt

from v03_pipeline.lib.model import ReferenceGenome


def get_contig_cov(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    contig: str,
    call_rate_threshold: float,
    af_threshold: float = 0.01,
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
    if contig in hl.get_reference(reference_genome.value).x_contigs:
        mt = mt.filter_rows(mt.locus.in_x_nonpar())
    if contig in hl.get_reference(reference_genome.value).y_contigs:
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
    :param xy_fstat_threshold: F-stat threshold above which a sample will be called XY. Default is 0.75
    :param xx_fstat_threshold: F-stat threshold below which a sample will be called XX. Default is 0.5
    :param aaf_threshold: Alternate allele frequency threshold for `hl.impute_sex`. Default is 0.05
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
    sex_ht = hl.impute_sex(
        mt.GT,
        aaf_threshold=aaf_threshold,
        male_threshold=xy_fstat_threshold,
        female_threshold=xx_fstat_threshold,
    )
    mt = mt.annotate_cols(**sex_ht[mt.col_key])
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


def call_sex(
    mt: hl.MatrixTable,
    reference_genome: ReferenceGenome,
    use_y_cov: bool = False,
    add_x_cov: bool = False,
    y_cov_threshold: float = 0.1,
    normalization_contig: str = '20',
    xy_fstat_threshold: float = 0.75,
    xx_fstat_threshold: float = 0.5,
    aaf_threshold: float = 0.05,
    call_rate_threshold: float = 0.25,
):
    # Filter to SNVs and biallelics
    # NB: We should already have filtered biallelics, but just
    mt = mt.filter_rows(hl.is_snp(mt.alleles[0], mt.alleles[1]))

    # Filter to PASS variants only (variants with empty or missing filter set)
    # TODO: Make this an optional argument before moving to gnomad_methods
    mt = mt.filter_rows(
        hl.is_missing(mt.filters) | (mt.filters.length() == 0), keep=True,
    )

    ht = run_hails_impute_sex(
        mt,
        reference_genome,
        xy_fstat_threshold,
        xx_fstat_threshold,
        aaf_threshold,
    )

    if use_y_cov:
        final_annotations.extend(
            [
                f"chr{normalization_contig}_mean_dp",
                "chrY_mean_dp",
                "normalized_y_coverage",
            ]
        )
        norm_ht = get_chr_cov(mt, "GRCh38", normalization_contig, call_rate_threshold)
        sex_ht = sex_ht.annotate(**norm_ht[sex_ht.s])
        chry_ht = get_chr_cov(mt, "GRCh38", "Y", call_rate_threshold)
        sex_ht = sex_ht.annotate(**chry_ht[sex_ht.s])
        sex_ht = sex_ht.annotate(
            normalized_y_coverage=hl.or_missing(
                sex_ht[f"chr{normalization_contig}_mean_dp"] > 0,
                sex_ht.chrY_mean_dp / sex_ht[f"chr{normalization_contig}_mean_dp"],
            )
        )
        if add_x_cov:
            final_annotations.extend(["chrX_mean_dp", "normalized_x_coverage"])
            chrx_ht = get_chr_cov(mt, "GRCh38", "X", call_rate_threshold)
            sex_ht = sex_ht.annotate(**chrx_ht[sex_ht.s])
            sex_ht = sex_ht.annotate(
                normalized_x_coverage=hl.or_missing(
                    sex_ht[f"chr{normalization_contig}_mean_dp"] > 0,
                    sex_ht.chrX_mean_dp / sex_ht[f"chr{normalization_contig}_mean_dp"],
                )
            )
        sex_ht = sex_ht.annotate(
            ambiguous_sex=hl.is_missing(sex_ht.is_female),
            sex_aneuploidy=(sex_ht.is_female)
            & hl.is_defined(sex_ht.normalized_y_coverage)
            & (sex_ht.normalized_y_coverage > y_cov_threshold)
            | (~sex_ht.is_female)
            & hl.is_defined(sex_ht.normalized_y_coverage)
            & (sex_ht.normalized_y_coverage < y_cov_threshold),
        )

        sex_expr = (
            hl.case()
            .when(sex_ht.ambiguous_sex, "ambiguous_sex")
            .when(sex_ht.sex_aneuploidy, "sex_aneuploidy")
            .when(sex_ht.is_female, "XX")
            .default("XY")
        )

    else:
        sex_ht = sex_ht.annotate(ambiguous_sex=hl.is_missing(sex_ht.is_female))
        sex_expr = hl.if_else(
            sex_ht.ambiguous_sex,
            "ambiguous_sex",
            hl.if_else(sex_ht.is_female, "XX", "XY"),
        )
    sex_ht = sex_ht.annotate(sex=sex_expr)
    sex_ht = sex_ht.select(*final_annotations)

    out_path = f"{out_bucket}/{mt_name}_sex.txt"
    sex_ht.export(out_path)
