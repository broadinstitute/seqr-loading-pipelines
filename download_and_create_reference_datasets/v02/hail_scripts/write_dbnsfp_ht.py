import hail as hl
from hail.expr import tint, tfloat, tstr

DBNSFP_INFO = {
    "2.9.3": {
        "source_path": "gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.chr*.gz",
        "output_path": "gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.ht"
    },
}

# Fields from the dataset file.
DBNSFP_SCHEMA = {
    '2.9.3': {
         '#chr': tstr,
         'pos(1-coor)': tint,
         'ref': tstr,
         'alt': tstr,
         'SIFT_pred': tstr,
         'Polyphen2_HDIV_pred': tstr,
         'Polyphen2_HVAR_pred': tstr,
         'LRT_pred': tstr,
         'MutationTaster_pred': tstr,
         'MutationAssessor_pred': tstr,
         'FATHMM_pred': tstr,
         'MetaSVM_pred': tstr,
         'MetaLR_pred': tstr,
         'VEST3_score': tstr,
         'VEST3_rankscore': tstr,
         'PROVEAN_pred': tstr,
         'M-CAP_pred': tstr,
         'REVEL_score': tstr,
         'REVEL_rankscore': tstr,
         'MutPred_Top5features': tstr,
         'Eigen-phred': tstr,
         'Eigen-PC-phred': tstr,
         'GERP++_RS': tstr,
         'GERP++_RS_rankscore': tstr,
         'phyloP46way_primate': tstr,
         'phyloP46way_primate_rankscore': tstr,
         'phyloP46way_placental': tstr,
         'phyloP46way_placental_rankscore': tstr,
         'phyloP100way_vertebrate': tstr,
         'phyloP100way_vertebrate_rankscore': tstr,
         'phastCons46way_primate': tstr,
         'phastCons46way_primate_rankscore': tstr,
         'phastCons46way_placental': tstr,
         'phastCons46way_placental_rankscore': tstr,
         'phastCons100way_vertebrate': tstr,
         'phastCons100way_vertebrate_rankscore': tstr,
         'SiPhy_29way_pi': tstr,
         'SiPhy_29way_logOdds_rankscore': tstr,
         'ESP6500_AA_AF': tfloat,
         # This space is intentional and in the file.
         'ESP6500_EA_AF ': tfloat,
         'ARIC5606_AA_AC': tint,
         'ARIC5606_AA_AF': tfloat,
         'ARIC5606_EA_AC': tint,
         'ARIC5606_EA_AF': tfloat,
    }
}

def generate_replacement_fields(ht, schema):
    '''
    Hail Tables need to have a fields remapping. This function generates a dict from
    the new transformed field name (whitespace stripped, dash to underscore) to original
    field name. The original field name references the exact attribute of ht, per
    hail construct so we can feed it to the select query.

    :param ht: Hail table to reference the original field attribute.
    :param schema: schema mapping from original field name to type
    :return: dict of new transformed name to old attr from ht
    '''
    def transform(field_name):
        return field_name.strip(" `#").replace("(1-coor)", "")\
            .replace("(1-based)", "").replace("-", "_").replace("+", "")
    return {
        transform(field_name): getattr(ht, field_name) for field_name in schema.keys()
    }

def dbnsfp_to_ht(source_path, output_path, dbnsfp_version="2.9.3"):
    '''
    Runs the conversion from importing the table from the source path, proessing the
    fields, and outputing as a matrix table to the output path.

    :param source_path: location of the dbnsfp data
    :param output_path: location to put the matrix table
    :param dbnsfp_version: version
    :return:
    '''
    # Import the table using the schema to define the types.
    ht = hl.import_table(source_path,
                         types=DBNSFP_SCHEMA[dbnsfp_version],
                         missing='.',
                         min_partitions=10000)
    # get a attribute map to run a select and remap fields.
    replacement_fields = generate_replacement_fields(ht, DBNSFP_SCHEMA[dbnsfp_version])
    ht = ht.select(**replacement_fields)
    ht = ht.filter(ht.alt == ht.ref, keep=False)
    # Needed for matrix table conversion to denote variant data.
    ht = ht.key_by(locus=hl.locus(ht.chr, ht.pos), alleles=[ht.ref, ht.alt])


    ht = ht.annotate_globals(
        sourceFilePath=source_path,
        version=dbnsfp_version,
    )

    ht.write(output_path)
    return ht

def run():
    for dbnsfp_version, config in DBNSFP_INFO.items():
        ht = dbnsfp_to_ht(config["source_path"],
                          config["output_path"],
                          dbnsfp_version)
        ht.describe()

run()
