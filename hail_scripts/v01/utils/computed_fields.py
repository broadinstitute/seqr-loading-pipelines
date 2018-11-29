CONSEQUENCE_TERMS = [
    "transcript_ablation",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "stop_gained",
    "frameshift_variant",
    "stop_lost",
    "start_lost",  # new in v81
    "initiator_codon_variant",  # deprecated
    "transcript_amplification",
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "protein_altering_variant",  # new in v79
    "splice_region_variant",
    "incomplete_terminal_codon_variant",
    "stop_retained_variant",
    "synonymous_variant",
    "coding_sequence_variant",
    "mature_miRNA_variant",
    "5_prime_UTR_variant",
    "3_prime_UTR_variant",
    "non_coding_transcript_exon_variant",
    "non_coding_exon_variant",  # deprecated
    "intron_variant",
    "NMD_transcript_variant",
    "non_coding_transcript_variant",
    "nc_transcript_variant",  # deprecated
    "TFBS_ablation",
    "TFBS_amplification",
    "TF_binding_site_variant",
    "regulatory_region_ablation",
    "regulatory_region_amplification",
    "feature_elongation",
    "regulatory_region_variant",
    "feature_truncation",
    "intergenic_variant",
]

OMIT_CONSEQUENCE_TERMS = [
    "upstream_gene_variant",
    "downstream_gene_variant",
]

# hail Dict expression that maps each CONSEQUENCE_TERM to it's rank in the list
CONSEQUENCE_TERM_RANKS = map(str, range(len(CONSEQUENCE_TERMS)))
CONSEQUENCE_TERM_RANK_LOOKUP = (
    "Dict(%s, %s)" % (CONSEQUENCE_TERMS, CONSEQUENCE_TERM_RANKS)
).replace("'", '"')


def get_expr_for_vep_sorted_transcript_consequences_array(vep_root="va.vep", include_coding_annotations=True, add_transcript_rank=True):
    """Sort transcripts by 3 properties:

        1. coding > non-coding
        2. transcript consequence severity
        3. canonical > non-canonical

    so that the 1st array entry will be for the coding, most-severe, canonical transcript (assuming
    one exists).

    Also, for each transcript in the array, computes these additional fields:
        domains: converts Array[Struct] to string of comma-separated domain names
        hgvs: set to hgvsp is it exists, or else hgvsc. TODO needs more work to match gnomAD browser logic.
        major_consequence: set to most severe consequence for that transcript (
            VEP sometimes provides multiple consequences for a single transcript)
        major_consequence_rank: major_consequence rank based on VEP SO ontology (most severe = 1)
            (see http://www.ensembl.org/info/genome/variation/predicted_data.html)
        category: set to one of: "lof", "missense", "synonymous", "other" based on the value of major_consequence.

    Args:
        vep_root (string): root path of the VEP struct in the VDS
        include_coding_annotations (bool): if True, fields relevant to protein-coding variants will be included
        add_transcript_rank (bool): if True, a 'transcript_rank' field will be added to each transcript Struct which
            records the transcript's ranking based on the above sort order.

    """

    coding_transcript_consequences = """
        amino_acids,
        biotype,
        canonical,
        cdna_start,
        cdna_end,
        codons,
        consequence_terms,
        domains,
        gene_id,
        transcript_id,
        protein_id,
        gene_symbol,
        hgvsc,
        hgvsp,
        lof,
        lof_flags,
        lof_filter
    """

    non_coding_transcript_consequences = """
        biotype,
        canonical,
        cdna_start,
        cdna_end,
        codons,
        consequence_terms,
        domains,
        gene_id,
        transcript_id,
        gene_symbol,
        hgvsc,
        hgvsp
    """

    if include_coding_annotations:
        transcript_consequences = coding_transcript_consequences
    else:
        transcript_consequences = non_coding_transcript_consequences

    result = """
    let CONSEQUENCE_TERM_RANK_LOOKUP = %(CONSEQUENCE_TERM_RANK_LOOKUP)s and
        OMIT_CONSEQUENCE_TERMS = %(OMIT_CONSEQUENCE_TERMS)s.toSet in %(vep_root)s.transcript_consequences.map(
            c => select(c, %(transcript_consequences)s)
        ).map(
            c => merge(
                drop(c, consequence_terms),
                {
                    consequence_terms: c.consequence_terms.filter(t => !OMIT_CONSEQUENCE_TERMS.contains(t))
                })
        ).filter(
            c => c.consequence_terms.size > 0
        ).map(
            c => merge(
                drop(c, domains),
                {
                    domains: c.domains.map( domain => domain.db+":"+domain.name ),
                    hgvs: orElse(c.hgvsp, c.hgvsc),
                    major_consequence: if(c.consequence_terms.size > 0)
                            c.consequence_terms.toArray.sortBy(t => CONSEQUENCE_TERM_RANK_LOOKUP.get(t).toInt)[0]
                        else
                            NA:String
                })
        ).map(c => merge(c, {
                major_consequence_rank: CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence).toInt,
                category:
                    if(CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence).toInt <= CONSEQUENCE_TERM_RANK_LOOKUP.get("frameshift_variant").toInt)
                        "lof"
                    else if(CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence).toInt <= CONSEQUENCE_TERM_RANK_LOOKUP.get("missense_variant").toInt)
                        "missense"
                    else if(CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence).toInt <= CONSEQUENCE_TERM_RANK_LOOKUP.get("synonymous_variant").toInt)
                        "synonymous"
                    else
                        "other"
            })
        ).sortBy(c => let
            is_coding = (orElse(c.biotype, "") == "protein_coding") and
            is_most_severe = c.consequence_terms.toSet.contains(%(vep_root)s.most_severe_consequence) and
            is_canonical = (orElse(c.canonical, 0) == 1) in

            if(is_coding)
                if(is_most_severe)
                    if(is_canonical)  1  else  2
                else
                    if(is_canonical)  3  else  4
            else
                if(is_most_severe)
                    if(is_canonical)  5  else  6
                else
                    if(is_canonical)  7  else  8
        )
    """ % dict(locals().items()+globals().items())

    if not include_coding_annotations:
        # for non-coding variants, drop fields here that are hard to exclude in the above code
        result += ".map(c => drop(c, domains, hgvsp))"

    if add_transcript_rank:
        result = """let processed_transcript_consequences = %(result)s in 
            range(processed_transcript_consequences.length).map(array_index => 
                merge(
                    processed_transcript_consequences[array_index],
                    { transcript_rank: array_index }
                )
            )
        """ % locals()

    return result


def get_expr_for_worst_transcript_consequence_annotations_struct(
        vep_sorted_transcript_consequences_root="va.vep.sorted_transcript_consequences",
        include_coding_annotations=True,
    ):
    """Retrieves the top-ranked transcript annotation based on the ranking computed by
    get_expr_for_vep_sorted_transcript_consequences_array(..)
    
    Args:
        vep_sorted_transcript_consequences_root (string):
        include_coding_annotations (bool): 
    """

    coding_transcript_consequences = """
        amino_acids,
        biotype,
        canonical,
        cdna_start,
        cdna_end,
        codons,
        gene_id,
        gene_symbol,
        hgvsc,
        hgvsp,
        lof,
        lof_flags,
        lof_filter,
        protein_id,
        transcript_id,
        domains,
        hgvs,
        major_consequence,
        major_consequence_rank,
        category
    """

    coding_transcript_consequences_with_types = """
        amino_acids:String,
        biotype:String,
        canonical:Int,
        cdna_start:Int,
        cdna_end:Int,
        codons:String,
        gene_id:String,
        gene_symbol:String,
        hgvsc:String,
        hgvsp:String,
        lof:String,
        lof_flags:String,
        lof_filter:String,
        protein_id:String,
        transcript_id:String,
        hgvs:String,
        major_consequence:String,
        major_consequence_rank:Int,
        category:String,
        domains:String
    """


    non_coding_transcript_consequences = """
        biotype,
        canonical,
        cdna_start,
        cdna_end,
        codons,
        gene_id,
        gene_symbol,
        hgvsc,
        transcript_id,
        hgvs,
        major_consequence,
        major_consequence_rank,
        category
    """
    
    non_coding_transcript_consequences_with_types = """
        biotype:String,
        canonical:Int,
        cdna_start:Int,
        cdna_end:Int,
        codons:String,
        gene_id:String,
        gene_symbol:String,
        hgvsc:String,
        transcript_id:String,
        hgvs:String,
        major_consequence:String,
        major_consequence_rank:Int,
        category:String
    """

    if include_coding_annotations:
        transcript_consequences = coding_transcript_consequences
        transcript_consequences_with_types = coding_transcript_consequences_with_types
    else:
        transcript_consequences = non_coding_transcript_consequences
        transcript_consequences_with_types = non_coding_transcript_consequences_with_types

    return """
    let NA_type = NA:Struct{ %(transcript_consequences_with_types)s } in
    if( %(vep_sorted_transcript_consequences_root)s.length == 0 )
        NA_type
    else
        let worst_transcript_consequence = select(%(vep_sorted_transcript_consequences_root)s[0], %(transcript_consequences)s) in
        merge(
            drop(worst_transcript_consequence, domains),
            { 
                domains: worst_transcript_consequence.domains.toSet.mkString(",") 
            }
        )
    """ % locals()


def get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.vep.sorted_transcript_consequences", only_coding_genes=False):
    """Expression to compute the set of gene ids in VEP annotations for this variant.

    Args:
        vep_transcript_consequences_root (string): path of VEP transcript_consequences root in the struct
        only_coding_genes (bool): If set to True, non-coding genes will be excluded.
    Return:
        string: expression
    """
    expr = "%(vep_transcript_consequences_root)s" % locals()
    if only_coding_genes:
        expr += ".filter( x => x.biotype == 'protein_coding')"
    expr += ".map( x => x.gene_id ).toSet"

    return expr


def get_expr_for_vep_transcript_ids_set(vep_transcript_consequences_root="va.vep.transcript_consequences"):
    return "%(vep_transcript_consequences_root)s.map( x => x.transcript_id ).toSet" % locals()


def get_expr_for_vep_consequence_terms_set(vep_transcript_consequences_root="va.vep.transcript_consequences"):
    return "%(vep_transcript_consequences_root)s.map( x => x.consequence_terms ).flatten.toSet" % locals()


def get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root="va.vep.transcript_consequences"):
    return """%(vep_transcript_consequences_root)s
        .filter( x => isDefined(x.domains) && x.domains.length > 0 )
        .map( x => x.domains )
        .flatten.toSet""" % locals()


def get_expr_for_vep_transcript_id_to_consequence_map(vep_transcript_consequences_root="va.vep.transcript_consequences"):
    return """[
        '{',
            %(vep_transcript_consequences_root)s
                .map( x =>  
                    ['"', x.transcript_id, '": "', x.major_consequence, '"'].mkString("") 
                ).mkString(", "),
        '}'
    ].mkString("")
    """ % locals()


def get_expr_for_variant_type():
    """Returns "I" (insertion), "D" (deletion), "S" (snp) or "M" (MNP)"""

    return """
        if(v.ref.length > v.alt.length) "D"
        else if (v.ref.length < v.alt.length) "I"
        else if (v.ref.length > 1) "M"
        else "S"
    """


def get_expr_for_contig(field_prefix="v."):
    """Normalized contig name"""
    return field_prefix+'contig.replace("chr", "")'


def get_expr_for_start_pos():
    return 'v.start'


def get_expr_for_ref_allele():
    return 'v.ref'


def get_expr_for_alt_allele():
    return 'v.alt'


def get_expr_for_orig_alt_alleles_set():
    """Compute an array of variant ids for each alt allele"""
    contig_expr = get_expr_for_contig()
    return 'v.altAlleles.map( a => %(contig_expr)s + "-" + v.start + "-" + v.ref + "-" + a.alt ).toSet' % locals()


def get_expr_for_variant_id(max_length=None):
    """Expression for computing <chrom>-<pos>-<ref>-<alt>

    Args:
        max_length: (optional) length at which to truncate the <chrom>-<pos>-<ref>-<alt> string

    Return:
        string: "<chrom>-<pos>-<ref>-<alt>"
    """
    contig_expr = get_expr_for_contig()
    if max_length is not None:
        return '(%(contig_expr)s + "-" + v.start + "-" + v.ref + "-" + v.alt)[0:%(max_length)s]' % locals()
    else:
        return '%(contig_expr)s + "-" + v.start + "-" + v.ref + "-" + v.alt' % locals()


def get_expr_for_contig_number(field_prefix="v."):
    """Convert contig name to contig number"""

    contig_expr = get_expr_for_contig(field_prefix)
    return """
        let contig = %(contig_expr)s in
            if(contig == "X") 23
            else if (contig == "Y") 24
            else if (contig[0] == "M") 25
            else contig.toInt
    """ % locals()


def get_expr_for_xpos(field_prefix="v.", pos_field="start"):
    """Genomic position represented as a single number = contig_number * 10**9 + position.
    This represents chrom:pos more compactly and allows for easier sorting.
    """
    contig_number_expr = get_expr_for_contig_number(field_prefix)
    return """
    let contig_number = %(contig_number_expr)s and pos = %(field_prefix)s%(pos_field)s in
        1000000000 * contig_number.toLong + pos
    """ % locals()


def get_expr_for_end_pos(field_prefix="v.", pos_field="start", ref_field="ref"):
    """Compute the end position based on start position and ref allele length"""
    return "%(field_prefix)s%(pos_field)s + %(field_prefix)s%(ref_field)s.length - 1" % locals()


def get_expr_for_end_pos_from_info_field(field_prefix="va.", end_field="info.END"):
    """Retrieve the "END" position from the INFO field. This is typically found in VCFs produced by
    SV callers."""
    return "%(field_prefix)s%(end_field)s" % locals()


def get_expr_for_filtering_allele_frequency(ac_field="va.AC[va.aIndex - 1]", an_field="va.AN", confidence_interval=0.95):
    """Compute the filtering allele frequency for the given AC, AN and confidence interval."""
    if not (0 < confidence_interval < 1):
        raise ValueError("Invalid confidence interval: %s. Confidence interval must be between 0 and 1." % confidence_interval)
    return "filtering_allele_frequency(%(ac_field)s, %(an_field)s, %(confidence_interval)s)" % locals()


def copy_field(vds, dest_field="va.pos", source_field="v.start"):
    """Copy a field from one place in the schema to another"""
    return vds.annotate_variants_expr(
        '%(root)s = %(source_field)s' % locals()
    )



