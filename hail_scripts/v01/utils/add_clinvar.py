import gzip
import hail
import logging
import os
from pprint import pprint

from kubernetes.shell_utils import simple_run as run

logger = logging.getLogger()

CLINVAR_FTP_PATH = "ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh{genome_version}/clinvar.vcf.gz"
CLINVAR_VDS_PATH = 'gs://seqr-reference-data/GRCh{genome_version}/clinvar/clinvar.GRCh{genome_version}.vds'

CLINVAR_FIELDS = """
     --- AF_ESP: Double,
     --- AF_EXAC: Double,
     --- AF_TGP: Double,
     ALLELEID: Int,
     --- CLNDN: Array[String],
     --- CLNDNINCL: Array[String],
     --- CLNDISDB: Array[String],
     --- CLNDISDBINCL: Array[String],
     --- CLNHGVS: Array[String],
     CLNREVSTAT: Array[String],
     CLNSIG: Array[String],
     --- CLNSIGCONF: Array[String],
     --- CLNSIGINCL: Array[String],
     --- CLNVC: String,
     --- CLNVCSO: String,
     --- CLNVI: Array[String],
     --- DBVARID: Array[String],
     --- GENEINFO: String,
     --- MC: Array[String],
     --- ORIGIN: Array[String],
     --- RS: Array[String],
     --- SSR: Int
"""

CLINVAR_GOLD_STARS_LOOKUP = """Dict(
    [
        'no_interpretation_for_the_single_variant', 'no_assertion_provided', 'no_assertion_criteria_provided',
        'criteria_provided,_single_submitter', 'criteria_provided,_conflicting_interpretations',
        'criteria_provided,_multiple_submitters,_no_conflicts', 'reviewed_by_expert_panel', 'practice_guideline'
    ],
    [
         0, 0, 0,
         1, 1,
         2, 3, 4
    ]
)"""


CLINVAR_VDS_CACHE = {}

def read_clinvar_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    if CLINVAR_VDS_CACHE.get((genome_version, subset)):
       return CLINVAR_VDS_CACHE[(genome_version, subset)]

    clinvar_vds_path = CLINVAR_VDS_PATH.format(genome_version=genome_version)
    logger.info("==> Reading in {}".format(clinvar_vds_path))
    clinvar_vds = hail_context.read(clinvar_vds_path)

    if subset:
        clinvar_vds = clinvar_vds.filter_intervals(hail.Interval.parse(subset))

    CLINVAR_VDS_CACHE[(genome_version, subset)] = clinvar_vds

    return clinvar_vds


def add_clinvar_to_vds(hail_context, vds, genome_version, root="va.clinvar", subset=None, verbose=True):
    """Add clinvar annotations to the vds.
    Also, if the clinvar VDS contains a globals 'version', set it as global.clinvarVersion on the returned vds.
    """

    clinvar_vds = read_clinvar_vds(hail_context, genome_version, subset=subset)

    clinvar_vds_meta = dict(clinvar_vds.globals._attrs)
    if 'version' in clinvar_vds_meta:
        vds = vds.annotate_global_expr('global.clinvarVersion = "{}"'.format(clinvar_vds_meta["version"]))

    # %(root)s.review_status = vds.info.CLNREVSTAT.toSet.mkString(","),
    vds = vds.annotate_variants_vds(clinvar_vds, expr="""
        %(root)s.allele_id = vds.info.ALLELEID,
        %(root)s.clinical_significance = vds.info.CLNSIG.toSet.mkString(","),
        %(root)s.gold_stars = %(CLINVAR_GOLD_STARS_LOOKUP)s.get(vds.info.CLNREVSTAT.toSet.mkString(","))
    """ % dict(locals().items()+globals().items()))

    return vds


def reset_clinvar_fields_in_vds(hail_context, vds, genome_version, root="va.clinvar", subset=None, verbose=True):
    """Add clinvar annotations to the vds"""

    clinvar_vds = read_clinvar_vds(hail_context, genome_version, subset=subset)

    # %(root)s.review_status = NA:String,
    vds = vds.annotate_variants_vds(clinvar_vds, expr="""
        %(root)s.allele_id = NA:Int,
        %(root)s.clinical_significance = NA:String,
        %(root)s.gold_stars = NA:Int
    """ % locals())

    return vds


def download_and_import_latest_clinvar_vcf(hail_context, genome_version, subset=None):
    """Downloads the latest clinvar VCF from the NCBI FTP server, copies it to HDFS and returns the hdfs file path
    as well the clinvar release date that's specified in the VCF header.

    Args:
        genome_version (str): "37" or "38"
        subset (str): subset by interval (eg. "X:12345-54321") - useful for testing
    Returns:
        2-tuple: (clinvar_vcf_hdfs_path, clinvar_release_date)
    """

    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    # download vcf
    clinvar_url = CLINVAR_FTP_PATH.format(genome_version=genome_version)
    local_tmp_file_path = "/tmp/clinvar_grch{}.vcf.gz".format(genome_version)
    clinvar_vcf_hdfs_path = "/tmp/" + os.path.basename(local_tmp_file_path)

    print("\n==> downloading {}".format(clinvar_url))

    run("wget {} -O {}".format(clinvar_url, local_tmp_file_path))

    run("hdfs dfs -copyFromLocal -f file://{} {}".format(local_tmp_file_path, clinvar_vcf_hdfs_path))

    clinvar_release_date = _parse_clinvar_release_date(local_tmp_file_path)

    # import vcf
    vds = hail_context.import_vcf(clinvar_vcf_hdfs_path, force_bgz=True, min_partitions=10000, drop_samples=True) #.filter_intervals(hail.Interval.parse("1-MT"))

    if subset:
        vds = vds.filter_intervals(hail.Interval.parse(subset))

    vds = vds.repartition(10000)  # because the min_partitions arg doesn't work in some cases
    vds = vds.annotate_global_expr('global.sourceFilePath = "{}"'.format(clinvar_url))
    vds = vds.annotate_global_expr('global.version = "{}"'.format(clinvar_release_date))

    # handle multi-allelics
    vds = vds.split_multi()

    # for some reason, this additional filter is necessary to avoid
    #  IllegalArgumentException: requirement failed: called altAllele on a non-biallelic variant
    vds = vds.filter_variants_expr("v.isBiallelic()", keep=True)

    print("\n==> downloaded clinvar vcf: ")
    pprint(vds.globals._attrs)

    return vds


def _parse_clinvar_release_date(local_vcf_path):
    """Parse clinvar release date from the VCF header.

    Args:
        local_vcf_path (str): clinvar vcf path on the local file system.

    Returns:
        str: return VCF release date as string, or None if release date not found in header.
    """
    print("==> parsing file release date")
    with gzip.open(local_vcf_path) as f:
        for line in f:
            if line.startswith("##fileDate="):
                clinvar_release_date = line.split("=")[-1].strip()
                return clinvar_release_date

            if not line.startswith("#"):
                return None
