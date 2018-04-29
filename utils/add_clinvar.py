from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr

CLINVAR_VDS_GRCH37 = 'gs://seqr-reference-data/GRCh37/clinvar/clinvar.GRCh37.vcf.gz'
CLINVAR_VDS_GRCH38 = 'gs://seqr-reference-data/GRCh38/clinvar/clinvar.GRCh38.vcf.gz'

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

# clinvar_clinical_significance (clinsig), clinvar_variation_id, review_status


def read_clinvar_vds(hail_context, genome_version, subset=None):
    if genome_version == "37":
        clinvar_vds_path = CLINVAR_VDS_GRCH37
    elif genome_version == "38":
        clinvar_vds_path = CLINVAR_VDS_GRCH38
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    clinvar_vds = hail_context.import_vcf(clinvar_vds_path, min_partitions=1000, force=True)

    if subset:
        import hail
        clinvar_vds = clinvar_vds.filter_intervals(hail.Interval.parse(subset))

    return clinvar_vds


def add_clinvar_to_vds(hail_context, vds, genome_version, root="va.clinvar", subset=None, verbose=True):
    """Add clinvar annotations to the vds"""

    clinvar_vds = read_clinvar_vds(hail_context, genome_version, subset=subset)

    vds = vds.annotate_variants_vds(clinvar_vds, expr="""
        %(root)s.variation_id = vds.info.ALLELEID,
        %(root)s.clinical_significance = vds.info.CLNSIG[0],
        %(root)s.review_status = vds.info.CLNREVSTAT[0]
    """ % locals())

    return vds
