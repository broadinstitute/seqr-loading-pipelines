import hail as hl
import logging
from pprint import pprint

# Note: Still using v01 code to not duplicate, remember to copy over before deprecating v01.
from hail_scripts.v01.utils.computed_fields import get_expr_for_orig_alt_alleles_set

logger = logging.getLogger()

def read_in_dataset(mt, input_path, dataset_type="VARIANTS", filter_interval=None, skip_summary=False, num_partitions=None, not_gatk_genotypes=False):
    """Utility method for reading in a .vcf or .vds dataset

    Args:
        hc (HailContext)
        input_path (str): vds or vcf input path
        dataset_type (str):
        filter_interval (str): optional chrom/pos filter interval (eg. "X:12345-54321")
        skip_summary (bool): don't run vds.summarize()
        num_partitions (int): if specified, runs vds.repartition(num_partitions)
        not_gatk_genotypes (bool): set to False if the dataset has the standard GATK genotype format (GT:AD:DP:GQ:PL)
    """
    input_path = input_path.rstrip("/")
    if input_path.endswith(".mt"):
        mt = hl.read_matrix_table(input_path)
    else:
        logger.info("\n==> import: " + input_path)

        if dataset_type == "VARIANTS":
            mt = hl.import_vcf(input_path, force_bgz=True, min_partitions=10000)
            # Do we have to do this is in v02?
            # vds_genotype_schema = str(vds.genotype_schema)
            # if vds_genotype_schema != "Genotype":
            #     if "DP:Int" in vds_genotype_schema and "GQ:Int" in vds_genotype_schema:
            #         vds = vds.annotate_genotypes_expr("g = Genotype(v, g.GT.gt, NA:Array[Int], g.DP, g.GQ, NA:Array[Int])")
            #     else:
            #         vds = vds.annotate_genotypes_expr("g = g.GT.toGenotype()")
        elif dataset_type == "SV":
            # what is a generic in 0.2? Is this still an issue?
            # mt = hl.import_vcf(input_path, force_bgz=True, min_partitions=10000, generic=True)
            pass
        else:
            raise ValueError("Unexpected dataset_type: %s" % dataset_type)

    num_repartition = num_partitions or max(mt.n_partitions(), 1000)
    if num_repartition is not mt.n_partitions():
        mt = mt.repartition(num_repartition, shuffle=True)

    if dataset_type == "VARIANTS":
        mt = hl.split_multi(mt)

    # this was commented out in v01 as well?
    #vds = vds.filter_alleles('v.altAlleles[aIndex-1].isStar()', keep=False) # filter star alleles

    if filter_interval is not None:
        logger.info("\n==> set filter interval to: %s" % (filter_interval, ))
        mt = hl.filter_intervals(mt, [hl.parse_locus_interval(filter_interval)])

    if not skip_summary and dataset_type == "VARIANTS":
        logger.info("Callset stats:")
        summary = hl.summarize_variants(mt, show=False)
        pprint(summary)
        total_variants = summary.n_variants
    else:
        total_variants = mt.count_rows()

    if dataset_type == "VARIANTS":
        mt = mt.annotate_rows(originalAltAlleles=get_expr_for_orig_alt_alleles_set())

    if not skip_summary:
        if total_variants == 0:
            raise ValueError("0 variants in VDS. Make sure chromosome names don't contain 'chr'")
        else:
            logger.info("\n==> total variants: %s" % (total_variants,))

    return mt
