from pprint import pformat

"""
Struct{
     `# Chromosome`: String,
     `Genomic Position Start`: Int,
     `Genomic Position End`: Int,
     `Cyto Location`: String,
     `Computed Cyto Location`: String,
     `Mim Number`: Int,
     `Gene Symbols`: String,
     `Gene Name`: String,
     `Approved Symbol`: String,
     `Entrez Gene ID`: String,
     `Ensembl Gene ID`: String,
     Comments: String,
     Phenotypes: String,
     `Mouse Gene Symbol/ID`: String
 }
"""

OMIM_GENEMAP2_PATH = 'gs://seqr-reference-data/omim/genemap2.txt.kt'


def add_omim_to_vds(hail_context, vds, root="va.omim", vds_key='va.mainTranscript.gene_id', verbose=True):
    """Add OMIM annotation"""

    kt = hail_context.read_table(OMIM_GENEMAP2_PATH) \
        .select(['Mim Number', 'Ensembl Gene ID']) \
        .rename({
            'Mim Number': 'mim_number',
            'Ensembl Gene ID': 'gene_id',
            #'Phenotypes': 'phenotypes',
        })

    if verbose:
        print("OMIM schema:\n" + pformat(kt.schema))

    return vds.annotate_variants_table(kt, root=root,  vds_key=vds_key)
