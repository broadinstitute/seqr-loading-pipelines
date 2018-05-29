from pprint import pformat

"""
Struct{
    transcript: String,
    gene: String,
    chr: String,
    n_exons: Int,
    cds_start: Int,
    cds_end: Int,
    bp: Int,
    mu_syn: Double,
    mu_mis: Double,
    mu_lof: Double,
    n_syn: Int,
    n_mis: Int,
    n_lof: Int,
    exp_syn: Double,
    exp_mis: Double,
    exp_lof: Double,
    syn_z: Double,
    mis_z: Double,
    lof_z: Double,
    pLI: Double,
    pRec: Double,
    pNull: Double
}
"""

GENE_CONSTRAINT_PATH = 'gs://seqr-reference-data/gene_constraint/fordist_cleaned_exac_r03_march16_z_pli_rec_null_data.txt.kt'


def add_gene_constraint_to_vds(hail_context, vds, root="va.gene_constraint", vds_key='va.mainTranscript.transcript_id', verbose=True):

    kt = hail_context.read_table(GENE_CONSTRAINT_PATH).select(['transcript', 'mis_z', 'pLI'])

    if verbose:
        print("Gene Constraint schema:\n" + pformat(kt.schema))

    return vds.annotate_variants_table(kt, root=root,  vds_key=vds_key)
