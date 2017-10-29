from  pprint import pprint
import hail
from hail.expr import TInt, TFloat, TString


def _parse_field_names_and_types(schema_string, to_keep=True, strip_quotes=True):
    """TODO: this is copied from vds_schema_string_utils. Figure out how to submit libs to hail"""
    for i, name_and_type in enumerate(schema_string.split(',')):
        name_and_type = name_and_type.strip()  # eg. "AF: Array[Double],"
        if not name_and_type:
            continue

        if (name_and_type.startswith("---") and to_keep) or (not name_and_type.startswith("---") and not to_keep):
            continue

        if len(name_and_type.split(": ")) != 2:
            raise ValueError("Could not parse name and type from line %s in schema: '%s'" % (i, name_and_type))

        field_name, field_type = name_and_type.strip(' -,').split(": ")
        if strip_quotes:
            field_name = field_name.strip("`")

        yield field_name, field_type # eg. ("AF", "Array[Double]")


hail_context = hail.HailContext()

CLINVAR_FILES = {
    "37": {
        "input_paths": [
            "gs://seqr-reference-data/GRCh37/clinvar/clinvar_alleles.multi.b37.tsv.gz",
            "gs://seqr-reference-data/GRCh37/clinvar/clinvar_alleles.single.b37.tsv.gz",
        ],
        "output_path": "gs://seqr-reference-data/GRCh37/clinvar/clinvar_alleles.b37.vds",
    },
    "38": {
        "input_paths": [
            "gs://seqr-reference-data/GRCh38/clinvar/clinvar_alleles.multi.b38.tsv.gz",
            "gs://seqr-reference-data/GRCh38/clinvar/clinvar_alleles.single.b38.tsv.gz",
        ],
        "output_path": "gs://seqr-reference-data/GRCh38/clinvar/clinvar_alleles.b38.vds",
    },
}

# this schema is the output of ./print_keytable_schema.py for the .tsv files above.
# columns with --- in front won't be included in the vds
CLINVAR_SCHEMA = """
    chrom: String,
    pos: Int,
    ref: String,
    alt: String,
    start: Int,
    stop: Int,
    --- strand: String,
    variation_type: String,
    variation_id: String,
    --- rcv: String,
    --- scv: String,
    allele_id: Int,
    --- symbol: String,
    --- hgvs_c: String,
    --- hgvs_p: String,
    --- molecular_consequence: String,
    clinical_significance: String,
    --- clinical_significance_ordered: String,
    --- pathogenic: Int,
    --- likely_pathogenic: Int,
    --- uncertain_significance: Int,
    --- likely_benign: Int,
    --- benign: Int,
    review_status: String,
    --- review_status_ordered: String,
    --- last_evaluated: String,
    all_submitters: String,
    --- submitters_ordered: String,
    all_traits: String,
    all_pmids: String,
    inheritance_modes: String,
    age_of_onset: String,
    prevalence: String,
    disease_mechanism: String,
    origin: String,
    xrefs: String,
    --- dates_ordered: String,
    gold_stars: String,
    conflicted: Int,
"""


TYPE_MAP = {
    'Int': TInt(),
    'String': TString(),
    'Float': TFloat(),
}


for genome_version in ["37", "38"]:
    input_paths = CLINVAR_FILES[genome_version]["input_paths"]
    output_path = CLINVAR_FILES[genome_version]["output_path"]

    fields_to_keep = _parse_field_names_and_types(CLINVAR_SCHEMA, to_keep=True)
    fields_to_drop = _parse_field_names_and_types(CLINVAR_SCHEMA, to_keep=False)

    field_types = {name: TYPE_MAP[type_string] for name, type_string in fields_to_keep}

    kt = (
        hail_context
            .import_table(
                input_paths,
                types=field_types,
                min_partitions=10000)
            .drop([name for name, _ in fields_to_drop])
            .filter("ref==alt", keep=False)
            .annotate("variant=Variant(chrom, pos, ref, alt)")
            .key_by('variant')
            .drop(["chrom", "pos", "ref", "alt"])
    )

    clinvar_vds = hail.VariantDataset.from_table(kt)
    clinvar_vds.write(output_path, overwrite=True)

    print("Wrote out clinvar file for GRCh%s: %s\n" % (genome_version, output_path))
    pprint(clinvar_vds.summarize())