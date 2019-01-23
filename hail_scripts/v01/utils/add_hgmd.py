import hail

HGMD_PATH_GRCh37 = 'gs://seqr-reference-data-private/GRCh37/HGMD/hgmd_pro_2018.4_hg19_without_db_field.vds'
HGMD_PATH_GRCh38 = 'gs://seqr-reference-data-private/GRCh38/HGMD/hgmd_pro_2018.4_hg38_without_db_field.vds'

def reset_hgmd_fields_in_vds(hail_context, vds, genome_version, root="va.hgmd", subset=None, verbose=True):
    hgmd_vds = read_hgmd_vds(hail_context, genome_version, subset=subset)

    # %(root)s.review_status = NA:String,
    vds = vds.annotate_variants_vds(hgmd_vds, expr="""
        %(root)s.accession = NA:String,
        %(root)s.class = NA:String
    """ % locals())

    return vds

HGMD_VDS_CACHE = {}

def read_hgmd_vds(hail_context, genome_version, subset=None):

    if genome_version == "37":
        hgmd_path = HGMD_PATH_GRCh37
    elif genome_version == "38":
        hgmd_path = HGMD_PATH_GRCh38
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    if HGMD_VDS_CACHE.get((genome_version, subset)):
        return HGMD_VDS_CACHE[(genome_version, subset)]

    if hgmd_path.endswith(".vds"):
        hgmd_vds = hail_context.read(hgmd_path)
    elif hgmd_path.endswith(".vcf") or hgmd_path.endswith(".vcf.gz"):
        hgmd_vds = hail_context.import_vcf(hgmd_path, force_bgz=True, min_partitions=2000)
    else:
        raise ValueError("Invalid HGMD path. File must end with '.vds' or '.vcf' or '.vcf.gz': " + str(hgmd_path))

    hgmd_vds =  hgmd_vds.split_multi()
    if subset:
        hgmd_vds = hgmd_vds.filter_intervals(hail.Interval.parse(subset))

    HGMD_VDS_CACHE[(genome_version, subset)] = hgmd_vds

    return hgmd_vds


def add_hgmd_to_vds(hail_context, vds, genome_version, root="va.hgmd", subset=None, verbose=True):

    hgmd_vds = read_hgmd_vds(hail_context, genome_version, subset=subset)

    vds = vds.annotate_variants_vds(hgmd_vds, expr="""
        %(root)s.accession = vds.rsid,
        %(root)s.class = vds.info.CLASS
    """ % locals())
    # %(root)s.rankscore = vds.info.RANKSCORE

    return vds
