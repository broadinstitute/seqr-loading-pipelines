import logging

VALIDATION_KEYTABLE_PATHS = {
    'coding_37': 'gs://seqr-reference-data/GRCh37/validate_vds/common_coding_variants.grch37.kt',
    'coding_38': 'gs://seqr-reference-data/GRCh38/validate_vds/common_coding_variants.grch38.kt',
    'noncoding_37': 'gs://seqr-reference-data/GRCh37/validate_vds/common_noncoding_variants.grch37.kt',
    'noncoding_38': 'gs://seqr-reference-data/GRCh38/validate_vds/common_noncoding_variants.grch38.kt',
}

logger = logging.getLogger(__name__)


def _read_common_gnomad_variants(hail_context, genome_version, coding_or_noncoding):
    """Returns a KeyTable of gnomAD genomes variants with AF > 0.9

    Args:
        genome_version (str): "37" or "38"
        coding_or_non_coding (str): "coding" or "noncoding"
    """

    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))
    if coding_or_noncoding not in ["coding", "noncoding"]:
        raise ValueError("Invalid coding_or_non_coding arg: " + str(coding_or_noncoding))

    return hail_context.read_table(VALIDATION_KEYTABLE_PATHS[coding_or_noncoding+"_"+genome_version])


def validate_vds_genome_version_and_sample_type(hail_context, vds, genome_version, sample_type):
    """Checks the VDS dataset_type by filtering for common gnomAD variants in non-coding regions

    Args:
        hail_context:
        vds: The vds to validate
        genome_version (str): "37" or "38"
        sample_type (str): "WES" or "WGS"

    Raises:
        ValueError: if vds inferred genome_version doesn't match
    """

    logger.info("\n==> validating vds genome_version={} and sample_type={}".format(genome_version, sample_type))
    gnomad_coding_variants_kt = _read_common_gnomad_variants(hail_context, genome_version, "coding")
    gnomad_noncoding_variants_kt = _read_common_gnomad_variants(hail_context, genome_version, "noncoding")

    _, matched_coding_variants = vds.filter_variants_table(gnomad_coding_variants_kt, keep=True).count()
    total_coding_variants = gnomad_coding_variants_kt.count()
    missing_common_coding_variants = matched_coding_variants/float(total_coding_variants) < 0.3
    logger.info("vds contains {} out of {} common coding variants".format(matched_coding_variants, total_coding_variants))

    _, matched_noncoding_variants = vds.filter_variants_table(gnomad_noncoding_variants_kt, keep=True).count()
    total_noncoding_variants = gnomad_noncoding_variants_kt.count()
    missing_common_noncoding_variants = matched_noncoding_variants/float(total_noncoding_variants) < 0.3
    logger.info("vds contains {} out of {} common non-coding variants".format(matched_noncoding_variants, total_noncoding_variants))

    if missing_common_coding_variants and missing_common_noncoding_variants:
        raise ValueError("genome version validation error: dataset specified as GRCh{genome_version} but doesn't contain "
            "the expected number of common GRCh{genome_version} variants".format(genome_version=genome_version))
    elif missing_common_coding_variants and not missing_common_noncoding_variants:
        # this should never happen. If it does, it probably means something's wrong with the list of common variants used for validation
        raise Exception("VDS is missing common coding variants but does contain common non-coding variants for GRCh{}".format(genome_version))
    elif not missing_common_coding_variants and not missing_common_noncoding_variants:
        imputed_sample_type = "WGS"
    elif not missing_common_coding_variants and missing_common_noncoding_variants:
        imputed_sample_type = "WES"

    if imputed_sample_type != sample_type:
        if imputed_sample_type == "WGS":
            raise ValueError("sample type validation error: dataset sample-type is specified as WES but appears to be "
                "WGS because it contains many common non-coding variants")
        elif imputed_sample_type == "WES":
            raise ValueError("sample type validation error: dataset sample-type is specified as WES but appears to be "
                "WGS because it's missing many common non-coding variants")
        else:
            raise Exception("Unexpected imputed_sample_type:" + imputed_sample_type)


def validate_vds_has_been_filtered(hail_context, vds):
    """Checks that the VDS has been filtered.

    Args:
        hail_context:
        vds: The vds to validate

    Raises:
        ValueError: if none of the variants have a FILTER value.
    """
    logger.info("\n==> check that callset has values in FILTER")
    num_filtered_variants = vds.filter_variants_expr("va.filters.isEmpty", keep=False).count_variants()
    logger.info("%d variants didn't PASS" % num_filtered_variants)
    if num_filtered_variants == 0:
        total_variants = vds.count_variants()
        if total_variants:
            raise ValueError("None of the %d variants have a FILTER value. seqr expects VQSR or hard-filtered callsets." % total_variants)
        else:
            raise ValueError("The callset is empty. No variants found.")