from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr


CLINVAR_FIELDS = """
    variation_type: String,
    variation_id: String,
    --- rcv: String,
    --- scv: String,
    allele_id: Int,
    clinical_significance: String,
    pathogenic: Int,
    likely_pathogenic: Int,
    uncertain_significance: Int,
    likely_benign: Int,
    benign: Int,
    conflicted: Int
    gold_stars: String,
    review_status: String,
    all_submitters: String,
    all_traits: String,
    all_pmids: String,
    inheritance_modes: String,
    age_of_onset: String,
    prevalence: String,
    disease_mechanism: String,
    origin: String,
    xrefs: String,
"""

def add_clinvar_to_vds(hail_context, vds, genome_version, root="va.clinvar", info_fields=CLINVAR_FIELDS, subset=None, verbose=True):
    """Add clinvar annotations to the vds"""

    if genome_version == "37":
        clinvar_vds_path = 'gs://seqr-reference-data/GRCh38/clinvar/clinvar_alleles.b37.vds'
    elif genome_version == "38":
        clinvar_vds_path = 'gs://seqr-reference-data/GRCh38/clinvar/clinvar_alleles.b38.vds'
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    clinvar_vds = hail_context.read(clinvar_vds_path)

    if subset:
        import hail
        clinvar_vds = clinvar_vds.filter_intervals(hail.Interval.parse(subset))

    expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=info_fields,
        other_source_root="vds",
    )

    if verbose:
        print(expr)
        #print("\n==> clinvar vds summary: ")
        #print("\n" + str(clinvar_vds.summarize()))

    vds = vds.annotate_variants_vds(clinvar_vds, expr=expr)

    return vds
