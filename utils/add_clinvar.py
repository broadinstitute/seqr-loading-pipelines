from utils.vds_schema_string_utils import convert_vds_schema_string_to_annotate_variants_expr


CLINVAR_FIELDS = """
    MEASURESET_TYPE: String,
    MEASURESET_ID: String,
    RCV: String,
    ALLELE_ID: String,
    CLINICAL_SIGNIFICANCE: String,
    PATHOGENIC: String,
    BENIGN: String,
    CONFLICTED: String,
    REVIEW_STATUS: String,
    GOLD_STARS: String,
    ALL_SUBMITTERS: String,
    ALL_TRAITS: String,
    ALL_PMIDS: String,
    INHERITANCE_MODES: String,
    AGE_OF_ONSET: String,
    PREVALENCE: String,
    DISEASE_MECHANISM: String,
    ORIGIN: String,
    XREFS: String
"""


def add_clinvar_to_vds(hail_context, vds, genome_version, root="va.clinvar", info_fields=CLINVAR_FIELDS, subset=None, verbose=True):
    """Add clinvar annotations to the vds"""

    if genome_version == "37":
        clinvar_single_vds = 'gs://seqr-reference-data/GRCh37/clinvar/clinvar_alleles.single.b37.vds'
        clinvar_multi_vds = 'gs://seqr-reference-data/GRCh37/clinvar/clinvar_alleles.multi.b37.vds'
    elif genome_version == "38":
        clinvar_single_vds = 'gs://seqr-reference-data/GRCh38/clinvar/clinvar_alleles.single.b38.vds'
        clinvar_multi_vds = 'gs://seqr-reference-data/GRCh38/clinvar/clinvar_alleles.multi.b38.vds'
    else:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    for clinvar_vds_path in [clinvar_single_vds, clinvar_multi_vds]:
        clinvar_vds = hail_context.read(clinvar_vds_path)

        if subset:
            import hail
            clinvar_vds = clinvar_vds.filter_intervals(hail.Interval.parse(subset))

        expr = convert_vds_schema_string_to_annotate_variants_expr(
            root=root,
            other_source_fields=info_fields,
            other_source_root="vds.info",
        )

        if verbose:
            print(expr)
            #print("\n==> clinvar vds summary: ")
            #print("\n" + str(clinvar_vds.summarize()))

        vds = vds.annotate_variants_vds(clinvar_vds, expr=expr)

    return vds
