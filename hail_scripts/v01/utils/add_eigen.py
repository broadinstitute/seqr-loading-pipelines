import hail

EIGEN_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/eigen/EIGEN_coding_noncoding.grch37.vds',
    '38': 'gs://seqr-reference-data/GRCh38/eigen/EIGEN_coding_noncoding.liftover_grch38.vds',
}


def read_eigen_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    eigen_vds = hail_context.read(EIGEN_VDS_PATHS[genome_version]).split_multi()

    if subset:
        eigen_vds = eigen_vds.filter_intervals(hail.Interval.parse(subset))

    return eigen_vds


def add_eigen_to_vds(hail_context, vds, genome_version, root="va.eigen", subset=None, verbose=True):

    eigen_vds = read_eigen_vds(hail_context, genome_version, subset=subset)

    vds = vds.annotate_variants_vds(eigen_vds, expr="%(root)s.Eigen_phred = vds.info.`Eigen-phred`" % locals())

    return vds
