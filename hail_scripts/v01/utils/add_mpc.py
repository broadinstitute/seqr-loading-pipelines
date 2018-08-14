import hail
from hail_scripts.v01.utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr


MPC_VDS_PATHS = {
    '37': 'gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.vds',
    '38': 'gs://seqr-reference-data/GRCh38/MPC/fordist_constraint_official_mpc_values.liftover.GRCh38.vds',
}

MPC_INFO_FIELDS = """
    MPC: Double,
    --- fitted_score: Double,
    --- mis_badness: Double,
    --- obs_exp: Double,
"""


def read_mpc_vds(hail_context, genome_version, subset=None):
    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    mpc_vds = hail_context.read(MPC_VDS_PATHS[genome_version]).split_multi()

    if subset:
        mpc_vds = mpc_vds.filter_intervals(hail.Interval.parse(subset))

    return mpc_vds

def add_mpc_to_vds(hail_context, vds, genome_version, root="va.mpc", info_fields=MPC_INFO_FIELDS, subset=None, verbose=True):
    """Add MPC annotations [Samocha 2017] to the vds"""

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=info_fields,
        other_source_root="vds.info",
    )

    if verbose:
        print(expr)
        #print("\n==> mpc summary: ")
        #print(mpc_vds.summarize())

    mpc_vds = read_mpc_vds(hail_context, genome_version, subset=subset)

    return vds.annotate_variants_vds(mpc_vds, expr=expr)
