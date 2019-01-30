import hail

SPLICE_AI_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/spliceai/spliceai_scores.vds',
    '38': 'gs://seqr-reference-data/GRCh38/spliceai/spliceai_scores.vds',
}


def read_splice_ai_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    splice_ai_vds = hail_context.read(SPLICE_AI_VDS_PATHS[genome_version]).split_multi()

    if subset:
        splice_ai_vds = splice_ai_vds.filter_intervals(hail.Interval.parse(subset))

    return splice_ai_vds


def add_splice_ai_to_vds(hail_context, vds, genome_version, root="va.splice_ai", subset=None, verbose=True):

    splice_ai_vds = read_splice_ai_vds(hail_context, genome_version, subset=subset)

    vds = vds.annotate_variants_vds(splice_ai_vds, expr="%(root)s.delta_score = vds.info.max_DS" % locals())

    return vds
