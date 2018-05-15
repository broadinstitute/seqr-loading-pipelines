

def read_eigen_vds(hail_context, genome_version, subset=None):

    if genome_version == "37":
        eigen_vds_path = 'gs://seqr-datasets/GRCh37/eigen/EIGEN_coding_noncoding.grch37.vds'
    elif genome_version == "38":
        eigen_vds_path = 'gs://seqr-datasets/GRCh38/eigen/EIGEN_coding_noncoding.liftover_grch38.vds'
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    eigen_vds = hail_context.read(eigen_vds_path).split_multi()

    if subset:
        import hail
        eigen_vds = eigen_vds.filter_intervals(hail.Interval.parse(subset))

    return eigen_vds


def add_eigen_to_vds(hail_context, vds, genome_version, root="va.eigen", subset=None, verbose=True):

    eigen_vds = read_eigen_vds(hail_context, genome_version, subset=subset)

    vds = vds.annotate_variants_vds(eigen_vds, expr="%(root)s.Eigen_phred = vds.info.Eigen-phred" % locals())

    return vds
