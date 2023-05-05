import logging
import sys
import matplotlib.pyplot as plt

import luigi
import hail as hl

from luigi_pipeline.lib.hail_tasks import (
    HailMatrixTableTask,
    GCSorLocalTarget,
)
from luigi_pipeline.seqr_loading import (
    check_if_path_exists,
)

logger = logging.getLogger(__name__)

FINAL_SEX_ANNOTATIONS = [
    "is_female",
    "f_stat",
    "n_called",
    "expected_homs",
    "observed_homs",
    "sex",
]
FEMALE_PLOIDY = "XX"
MALE_PLOIDY = "XY"
SEX_PLOIDY_ANNOTATIONS = [
    "is_female",
    "f_stat",
    "n_called",
    "expected_homs",
    "observed_homs",
    "sex",
]


class ValidateVCFTask(HailMatrixTableTask):
    """
    Inherits from a Hail MT Class to get helper function logic. Main logic to do annotations here.
    """

    source_paths = luigi.Parameter(
        default="[]",
        description="Path or list of paths of VCFs to be loaded.",  # NOTE: Not sure when default is in quotes
    )
    wes_filter_source_paths = luigi.OptionalParameter(
        default="[]", description="Path to delivered VCFs with filter annotation"
    )
    dest_path = luigi.Parameter(
        description="Path to write the matrix table."
    )  # TODO: Rename this to be task specific or use output()?
    temp_dir = luigi.Parameter(
        description="Path to write the temporary output. End with '/'",
        default="gs://seqr-scratch-temp/",
    )
    sample_type = luigi.ChoiceParameter(
        choices=["WGS", "WES"],
        description="Sample type, WGS or WES",
        var_type=str,
    )
    dont_validate = luigi.BoolParameter(
        description="Disable checking whether the dataset matches the specified "
        "genome version and WGS vs. WES sample type."
    )
    genome_version = luigi.ChoiceParameter(
        description="Reference Genome Version (37 or 38)", choices=["37", "38"]
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        # first validate paths
        if self.temp_dir:
            hl.init(
                tmp_dir=self.temp_dir,
            )  # Need to use the GCP bucket as temp storage for very large callset joins

        for source_path in self.source_paths:
            check_if_path_exists(source_path, "source_path")
        for wes_filter_source_path in self.wes_filter_source_paths:
            check_if_path_exists(source_path, "source_path")
        if self.temp_dir:
            check_if_path_exists(self.temp_dir, "temp_dir")
        mt = self.import_vcf()

        if not self.dont_validate:
            self.validate_mt(mt, self.genome_version, self.sample_type)
        mt.write(self.output().path)


class SexInferenceTask(luigi.Task):
    """
    Runs sex inference on a matrix table and writes the output.
    """

    temp_dir = luigi.Parameter(
        description="Path to write the temporary output. End with '/'",
        default="gs://seqr-scratch-temp/",
    )
    use_y_cov = luigi.BoolParameter(
        description="Whether to use chromosome Y coverage when inferring sex. Note that Y coverage is required to infer sex aneuploidies."
    )
    add_x_cov = luigi.BoolParameter(
        description="Whether to also calculate chromosome X mean coverage. Must be specified with use-y-cov."
    )
    y_cov_threshold = luigi.FloatParameter(
        default=0.1,
        description="Y coverage threshold used to infer sex aneuploidies (XY samples below and XX samples above this threshold will be inferred as having aneuploidies).",
    )
    xy_fstat_threshold = luigi.FloatParameter(
        default=0.75,
        description="F-stat threshold above which a sample will be called XY. Default is 0.75.",
    )
    xx_fstat_threshold = luigi.FloatParameter(
        default=0.50,
        description="F-stat threshold below which a sample will be called XX. Default is 0.50.",
    )
    aaf_threshold = luigi.FloatParameter(
        default=0.05,
        description="Alternate allele frequency threshold for `hl.impute_sex`. Default is 0.05.",
    )
    callrate_threshold = luigi.FloatParameter(
        default=0.25,
        description="Minimum variant call rate threshold. Default is 0.25.",
    )
    normalization_contig = luigi.Parameter(
        default="20",
        description="Autosome to use to normalize sex chromosome coverage. Default is chromosome 20.",
    )

    upstream_task = luigi.TaskParameter(
        description="Upstream task that task is dependent on."
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def requires(self):
        return self.upstream_task

    def temp_directory(self):
        return self.temp_dir

    def output(self):
        return GCSorLocalTarget(
            f"{self.input().path[:-3]}/sex_inference/sex_ploidy_check.ht"
        )

    def complete(self):
        # Complete is called by Luigi to check if the task is done and will skip if it is.
        # By default it checks to see that the output exists, but we want to check for the
        # _SUCCESS file to make sure it was not terminated halfway.
        return GCSorLocalTarget(f"{self.output().path}/_SUCCESS").exists()

    def mt(self):
        return hl.read_matrix_table(self.input().path)

    def run(self):
        self.call_sex(
            self.mt(),
            self.use_y_cov,
            self.add_x_cov,
            self.y_cov_threshold,
            self.normalization_contig,
            self.xy_fstat_threshold,
            self.xx_fstat_threshold,
            self.aaf_threshold,
            self.callrate_threshold,
        )

    def call_sex(
        self,
        mt: hl.MatrixTable,
        use_y_cov: bool,
        add_x_cov: bool,
        y_cov_threshold: float,
        normalization_contig: str,
        xy_fstat_threshold: float,
        xx_fstat_threshold: float,
        aaf_threshold: float,
        call_rate_threshold: float,
    ):
        """
        Call sex for the samples in a given callset and export results file to the desired path.

        :param MatrixTable mt: Input MatrixTable
        :param use_y_cov: Set to True to calculate and use chrY coverage for sex inference. Default is False
        :param add_x_cov: Set to True to calculate chrX coverage. Must be specified with use_y_cov. Default is False
        :param y_cov_threshold: Y coverage threshold used to infer sex aneuploidies.
        XY samples below and XX samples above this threshold will be inferred as having aneuploidies.
        Default is 0.1
        :param normalization_contig: Chosen chromosome for calculating normalized coverage. Default is "20"
        :param xy_fstat_threshold: F-stat threshold above which a sample will be called XY. Default is 0.75
        :param xx_fstat_threshold: F-stat threshold below which a sample will be called XX. Default is 0.5
        :param aaf_threshold: Alternate allele frequency threshold for `hl.impute_sex`. Default is 0.05
        :param call_rate_threshold: Minimum required call rate. Default is 0.25
        """
        # Read in matrix table and define output file name prefix
        logger.info("Using chromosome Y coverage? %s", use_y_cov)
        ref_genome = mt.locus.dtype.reference_genome.name

        # Filter to SNVs and biallelics
        mt = mt.filter_rows(
            (hl.len(mt.alleles) == 2) & hl.is_snp(mt.alleles[0], mt.alleles[1])
        )

        # Filter to PASS variants only (variants with empty or missing filter set)
        mt = mt.filter_rows(
            hl.is_missing(mt.filters) | (mt.filters.length() == 0), keep=True
        )
        print(self.input().path[:-3])
        logger.info("Inferring sex...")
        sex_ht = self.run_hails_impute_sex(
            mt,
            ref_genome,
            self.input().path[:-3],
            xy_fstat_threshold,
            xx_fstat_threshold,
            aaf_threshold,
        )
        sex_ht = sex_ht.checkpoint(
            f"{self.temp_directory()}temp_sex.ht", overwrite=True
        )

        if use_y_cov:
            logger.info("Calculating normalized Y coverage...")
            final_annotations = [
                *FINAL_SEX_ANNOTATIONS,
                *[
                    f"chr{normalization_contig}_mean_dp",
                    "chrY_mean_dp",
                    "normalized_y_coverage",
                ],
            ]
            norm_ht = self.get_chr_cov(
                mt, ref_genome, normalization_contig, call_rate_threshold
            )
            sex_ht = sex_ht.annotate(**norm_ht[sex_ht.s])
            chry_ht = self.get_chr_cov(mt, ref_genome, "Y", call_rate_threshold)
            sex_ht = sex_ht.annotate(**chry_ht[sex_ht.s])
            sex_ht = sex_ht.annotate(
                normalized_y_coverage=hl.or_missing(
                    sex_ht[f"chr{normalization_contig}_mean_dp"] > 0,
                    sex_ht.chrY_mean_dp / sex_ht[f"chr{normalization_contig}_mean_dp"],
                )
            )
            if add_x_cov:
                final_annotations.extend(["chrX_mean_dp", "normalized_x_coverage"])
                chrx_ht = self.get_chr_cov(mt, ref_genome, "X", call_rate_threshold)
                sex_ht = sex_ht.annotate(**chrx_ht[sex_ht.s])
                sex_ht = sex_ht.annotate(
                    normalized_x_coverage=hl.or_missing(
                        sex_ht[f"chr{normalization_contig}_mean_dp"] > 0,
                        sex_ht.chrX_mean_dp
                        / sex_ht[f"chr{normalization_contig}_mean_dp"],
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
        sex_ht.write(self.output().path, stage_locally=True, overwrite=True)

    def run_hails_impute_sex(
        self,
        mt: hl.MatrixTable,
        build: str,
        out_bucket: str,
        xy_fstat_threshold: float = 0.75,
        xx_fstat_threshold: float = 0.5,
        aaf_threshold: float = 0.05,
    ) -> hl.Table:
        """
        Impute sex, annotate MatrixTable with results, and output a histogram of fstat values.
        :param MatrixTable mt: MatrixTable containing samples to be ascertained for sex
        :param build: Reference used, either GRCh37 or GRCh38
        :param out_bucket: Bucket name for f-stat histogram
        :param xy_fstat_threshold: F-stat threshold above which a sample will be called XY. Default is 0.75
        :param xx_fstat_threshold: F-stat threshold below which a sample will be called XX. Default is 0.5
        :param aaf_threshold: Alternate allele frequency threshold for `hl.impute_sex`. Default is 0.05
        :return: Table with imputed sex annotations
        """

        logger.warning(
            "User needs to confirm fstat thresholds are still accurate for XY/XX by looking at fstat plots!"
        )
        ref_genome = mt.locus.dtype.reference_genome.name
        # Filter to the X chromosome and impute sex
        mt = hl.filter_intervals(
            mt,
            [
                hl.parse_locus_interval(
                    "X" if ref_genome == "GRCh37" else "chrX",
                    reference_genome=ref_genome,
                )
            ],
        )
        sex_ht = hl.impute_sex(
            mt.GT,
            aaf_threshold=aaf_threshold,
            male_threshold=xy_fstat_threshold,
            female_threshold=xx_fstat_threshold,
        )
        mt = mt.annotate_cols(**sex_ht[mt.col_key])
        sex_ht = mt.cols()
        sex_ht.describe()

        # Plot histogram of fstat values
        df = sex_ht.to_pandas()
        plt.clf()
        plt.hist(df["f_stat"])
        plt.xlabel("Fstat")
        plt.ylabel("Frequency")
        plt.axvline(xy_fstat_threshold, color="blue", linestyle="dashed", linewidth=1)
        plt.axvline(xx_fstat_threshold, color="red", linestyle="dashed", linewidth=1)

        out_path = f"{out_bucket}/sex_inference/fstat_histogram.png"
        with hl.hadoop_open(out_path, "wb") as out:
            plt.savefig(out)

        return sex_ht

    def get_chr_cov(
        self,
        mt: hl.MatrixTable,
        chr_name: str,
        call_rate_threshold: float = 0.25,
        af_threshold: float = 0.01,
        af_field: str = "AF",
    ) -> hl.Table:
        """
        Calculate mean chromosome coverage.
        :param mt: MatrixTable containing samples with chrY variants
        :param build: Reference used, either GRCh37 or GRCh38
        :param chr_name: Chosen chromosome. Must be either autosome (number only) or sex chromosome (X, Y)
        :param call_rate_threshold: Minimum call rate threshold. Default is 0.25
        :param af_threshold: Minimum allele frequency threshold. Default is 0.01
        :param af_field: Name of field containing allele frequency information. Default is "AF"
        :return: Table annotated with mean coverage of specified chromosome
        """
        logger.info(
            "Filtering to chromosome (and filtering to non-par regions if chromosome is X or Y)..."
        )
        ref_genome = mt.locus.dtype.reference_genome.name

        if chr_name == "Y":
            chr_place = 23
        elif chr_name == "X":
            chr_place = 22
        else:
            try:
                # Chromosome index in '.contigs' list should be one less than the chromosome number
                chr_place = int(chr_name) - 1
            except ValueError:
                logger.error("chr_name cannot be converted to an integer")
                return -99

        chr_name = hl.get_reference(ref_genome).contigs[chr_place]
        sex_mt = hl.filter_intervals(
            mt,
            [hl.parse_locus_interval(chr_name, reference_genome=ref_genome)],
        )

        if chr_place == 22:
            sex_mt = sex_mt.filter_rows(sex_mt.locus.in_x_nonpar())
        if chr_place == 23:
            sex_mt = sex_mt.filter_rows(sex_mt.locus.in_y_nonpar())

        # Filter to common SNVs above defined callrate (should only have one index in the array because the MT only contains biallelic variants)
        # TODO: Make callrate filtering optional before adding code to gnomad_methods
        sex_mt = sex_mt.filter_rows(sex_mt[af_field] > af_threshold)
        sex_mt = hl.variant_qc(sex_mt)
        sex_mt = sex_mt.filter_rows(sex_mt.variant_qc.call_rate > call_rate_threshold)

        logger.info("Returning mean coverage on chromosome %s...", chr_name)
        sex_mt = sex_mt.annotate_cols(**{f"{chr_name}_mean_dp": hl.agg.mean(sex_mt.DP)})
        return sex_mt.cols()


class WorkflowTask(luigi.Task):
    """
    Runs the entire workflow.
    """

    source_paths = luigi.Parameter(
        default="[]",
        description="Path or list of paths of VCFs to be loaded.",  # NOTE: Not sure when default is in quotes
    )
    wes_filter_source_paths = luigi.OptionalParameter(
        default="[]", description="Path to delivered VCFs with filter annotation"
    )
    dest_path = luigi.Parameter(
        description="Path to write the matrix table."
    )  # TODO: Rename thjis to be task specific or use output()?
    temp_dir = luigi.Parameter(
        description="Path to write the temporary output. End with '/'",
        default="gs://seqr-scratch-temp/",
    )
    genome_version = luigi.ChoiceParameter(
        description="Reference Genome Version (37 or 38)",
        choices=["37", "38"],
        default="38",
    )
    sample_type = luigi.ChoiceParameter(
        choices=["WGS", "WES"], description="Sample type, WGS or WES", var_type=str
    )
    dont_validate = luigi.BoolParameter(
        description="Disable checking whether the dataset matches the specified "
        "genome version and WGS vs. WES sample type."
    )
    genome_version = luigi.ChoiceParameter(
        description="Reference Genome Version (37 or 38)", choices=["37", "38"]
    )
    use_y_cov = luigi.BoolParameter(
        description="Whether to use chromosome Y coverage when inferring sex. Note that Y coverage is required to infer sex aneuploidies."
    )
    add_x_cov = luigi.BoolParameter(
        description="Whether to also calculate chromosome X mean coverage. Must be specified with use-y-cov."
    )
    y_cov_threshold = luigi.FloatParameter(
        default=0.1,
        description="Y coverage threshold used to infer sex aneuploidies (XY samples below and XX samples above this threshold will be inferred as having aneuploidies).",
    )
    xy_fstat_threshold = luigi.FloatParameter(
        default=0.75,
        description="F-stat threshold above which a sample will be called XY. Default is 0.75.",
    )
    xx_fstat_threshold = luigi.FloatParameter(
        default=0.50,
        description="F-stat threshold below which a sample will be called XX. Default is 0.50.",
    )
    aaf_threshold = luigi.FloatParameter(
        default=0.05,
        description="Alternate allele frequency threshold for `hl.impute_sex`. Default is 0.05.",
    )
    callrate_threshold = luigi.FloatParameter(
        default=0.25,
        description="Minimum variant call rate threshold. Default is 0.25.",
    )
    normalization_contig = luigi.Parameter(
        default="20",
        description="Autosome to use to normalize sex chromosome coverage. Default is chromosome 20.",
    )

    def requires(self):
        validate_vcf_task = ValidateVCFTask(
            source_paths=self.source_paths,
            wes_filter_source_paths=self.wes_filter_source_paths,
            dest_path=self.dest_path,
            temp_dir=self.temp_dir,
            sample_type=self.sample_type,
            genome_version=self.genome_version,
        )
        sex_inference_task = SexInferenceTask(
            upstream_task=validate_vcf_task,
            temp_dir=self.temp_dir,
            use_y_cov=self.use_y_cov,
            add_x_cov=self.add_x_cov,
            y_cov_threshold=self.y_cov_threshold,
            xy_fstat_threshold=self.xy_fstat_threshold,
            xx_fstat_threshold=self.xx_fstat_threshold,
            aaf_threshold=self.aaf_threshold,
            callrate_threshold=self.callrate_threshold,
            normalization_contig=self.normalization_contig,
        )
        return sex_inference_task

    def output(self):
        return self.input()


if __name__ == "__main__":
    # If run does not succeed, exit with 1 status code.
    luigi.run() or sys.exit(1)
