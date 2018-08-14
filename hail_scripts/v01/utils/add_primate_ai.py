import hail

PRIMATE_AI_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/primate_ai/PrimateAI_scores_v0.2.vds',
    '38': 'gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.vds',
}


def read_primate_ai_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    primate_ai_vds = hail_context.read(PRIMATE_AI_VDS_PATHS[genome_version]).split_multi()

    if subset:
        primate_ai_vds = primate_ai_vds.filter_intervals(hail.Interval.parse(subset))

    return primate_ai_vds


def add_primate_ai_to_vds(hail_context, vds, genome_version, root="va.primate_ai", subset=None, verbose=True):

    primate_ai_vds = read_primate_ai_vds(hail_context, genome_version, subset=subset)

    vds = vds.annotate_variants_vds(primate_ai_vds, expr="%(root)s.score = vds.info.score" % locals())

    return vds
