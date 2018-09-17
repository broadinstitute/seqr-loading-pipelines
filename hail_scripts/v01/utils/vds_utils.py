import hail
import logging
from pprint import pprint

from hail_scripts.v01.utils.computed_fields import get_expr_for_variant_id, get_expr_for_orig_alt_alleles_set
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

logger = logging.getLogger()


def compute_minimal_schema(vds, dataset_type="VARIANTS"):

    # add computed annotations
    vds = vds.annotate_variants_expr([
        "va.docId = %s" % get_expr_for_variant_id(512),
    ])

    #pprint(vds.variant_schema)

    # apply schema to dataset
    INPUT_SCHEMA  = {}
    if dataset_type == "VARIANTS":
        INPUT_SCHEMA["top_level_fields"] = """
            docId: String,
            wasSplit: Boolean,
            aIndex: Int,
        """

        INPUT_SCHEMA["info_fields"] = ""

    elif dataset_type == "SV":
        INPUT_SCHEMA["top_level_fields"] = """
            docId: String,
        """

        INPUT_SCHEMA["info_fields"] = ""

    else:
        raise ValueError("Unexpected dataset_type: %s" % dataset_type)

    expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)
    vds = vds.annotate_variants_expr(expr=expr)
    vds = vds.annotate_variants_expr("va = va.clean")

    return vds


def read_in_dataset(hc, input_path, dataset_type="VARIANTS", filter_interval=None, skip_summary=False):
    """Utility method for reading in a .vcf or .vds dataset

    Args:
        hc (HailContext)
        input_path (str): vds or vcf input path
        dataset_type (str):
        filter_interval (str): Optional chrom/pos filter interval (eg. "X:12345-54321")
    """
    input_path = input_path.rstrip("/")
    if input_path.endswith(".vds"):
        vds = read_vds(hc, input_path)
    else:
        logger.info("\n==> import: " + input_path)

        if dataset_type == "VARIANTS":
            vds = hc.import_vcf(input_path, force_bgz=True, min_partitions=10000)
        elif dataset_type == "SV":
            vds = hc.import_vcf(input_path, force_bgz=True, min_partitions=10000, generic=True)
        else:
            raise ValueError("Unexpected dataset_type: %s" % dataset_type)

        # ensure that va.wasSplit and va.aIndex are defined before calling split_multi() since split_multi() doesn't define these if all variants are bi-allelic
        vds = vds.annotate_variants_expr('va.wasSplit=false, va.aIndex=1')

    if vds.num_partitions() < 1000:
        vds = vds.repartition(1000, shuffle=True)

    vds = vds.split_multi()

    #vds = vds.filter_alleles('v.altAlleles[aIndex-1].isStar()', keep=False) # filter star alleles

    if filter_interval is not None:
        logger.info("\n==> set filter interval to: %s" % (filter_interval, ))
        vds = vds.filter_intervals(hail.Interval.parse(filter_interval))

    if not skip_summary:
        logger.info("Callset stats:")
        summary = vds.summarize()
        pprint(summary)
        total_variants = summary.variants
    else:
        total_variants = vds.count_variants()

    if dataset_type == "VARIANTS":
        vds = vds.annotate_variants_expr("va.originalAltAlleles=%s" % get_expr_for_orig_alt_alleles_set())

    if not skip_summary:
        if total_variants == 0:
            raise ValueError("0 variants in VDS. Make sure chromosome names don't contain 'chr'")
        else:
            logger.info("\n==> total variants: %s" % (total_variants,))

    return vds


def read_vds(hail_context, vds_path):
    logger.info("\n==> read in: {}".format(vds_path))

    return hail_context.read(vds_path)


def write_vds(vds, output_path, overwrite=True):
    """Writes the given VDS to the given output path"""

    logger.info("\n==> write out: " + output_path)

    vds.write(output_path, overwrite=overwrite)


def run_vep(vds, genome_version, root='va.vep', block_size=1000):
    if genome_version == "37":
        vds = vds.annotate_global_expr('global.gencodeVersion = "19"')  # see
    elif genome_version == "38":
        vds = vds.annotate_global_expr('global.gencodeVersion = "25"')  # see gs://hail-common/vep/vep/homo_sapiens/85_GRCh38/info.txt

    vds = vds.vep(config="/vep/vep-gcloud-grch{}.properties".format(genome_version), root=root, block_size=block_size)

    logger.info("==> Done with VEP")
    return vds
