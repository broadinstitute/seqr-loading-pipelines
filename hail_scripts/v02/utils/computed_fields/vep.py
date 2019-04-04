import hail as hl


# Consequence terms in order of severity (more severe to less severe) as estimated by Ensembl.
# See https://ensembl.org/info/genome/variation/prediction/predicted_data.html
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
    "start_retained_variant",
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
    "upstream_gene_variant",
    "downstream_gene_variant",
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

# hail DictExpression that maps each CONSEQUENCE_TERM to it's rank in the list
CONSEQUENCE_TERM_RANK_LOOKUP = hl.dict({term: rank for rank, term in enumerate(CONSEQUENCE_TERMS)})


OMIT_CONSEQUENCE_TERMS = [
    "upstream_gene_variant",
    "downstream_gene_variant",
]

def get_expr_for_vep_consequence_terms_set(vep_transcript_consequences_root):
    return hl.set(vep_transcript_consequences_root.flatmap(lambda c: c.consequence_terms))


def get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root, only_coding_genes=False):
    """Expression to compute the set of gene ids in VEP annotations for this variant.

    Args:
        vep_transcript_consequences_root (ArrayExpression): VEP transcript_consequences root in the struct
        only_coding_genes (bool): If set to True, non-coding genes will be excluded.
    Return:
        SetExpression: expression
    """

    expr = vep_transcript_consequences_root

    if only_coding_genes:
        expr = expr.filter(lambda c: hl.or_else(c.biotype, "") == "protein_coding")

    return hl.set(expr.map(lambda c: c.gene_id))


def get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root):
    return hl.set(
        vep_transcript_consequences_root.flatmap(lambda c: c.domains.map(lambda domain: domain.db + ":" + domain.name))
    )


PROTEIN_LETTERS_1TO3 = hl.dict(
    {
        "A": "Ala",
        "C": "Cys",
        "D": "Asp",
        "E": "Glu",
        "F": "Phe",
        "G": "Gly",
        "H": "His",
        "I": "Ile",
        "K": "Lys",
        "L": "Leu",
        "M": "Met",
        "N": "Asn",
        "P": "Pro",
        "Q": "Gln",
        "R": "Arg",
        "S": "Ser",
        "T": "Thr",
        "V": "Val",
        "W": "Trp",
        "Y": "Tyr",
        "X": "Ter",
        "*": "Ter",
        "U": "Sec",
    }
)


HGVSC_CONSEQUENCES = hl.set(["splice_donor_variant", "splice_acceptor_variant", "splice_region_variant"])


def get_expr_for_formatted_hgvs(csq):
    return hl.cond(
        hl.is_missing(csq.hgvsp) | HGVSC_CONSEQUENCES.contains(csq.major_consequence),
        csq.hgvsc.split(":")[-1],
        hl.cond(
            csq.hgvsp.contains("=") | csq.hgvsp.contains("%3D"),
            hl.bind(
                lambda protein_letters: "p." + protein_letters + hl.str(csq.protein_start) + protein_letters,
                hl.delimit(csq.amino_acids.split("").map(lambda l: PROTEIN_LETTERS_1TO3.get(l)), ""),
            ),
            csq.hgvsp.split(":")[-1],
        ),
    )


def get_expr_for_vep_sorted_transcript_consequences_array(vep_root,
                                                          include_coding_annotations=True,
                                                          omit_consequences=OMIT_CONSEQUENCE_TERMS):
    """Sort transcripts by 3 properties:

        1. coding > non-coding
        2. transcript consequence severity
        3. canonical > non-canonical

    so that the 1st array entry will be for the coding, most-severe, canonical transcript (assuming
    one exists).

    Also, for each transcript in the array, computes these additional fields:
        domains: converts Array[Struct] to string of comma-separated domain names
        hgvs: set to hgvsp is it exists, or else hgvsc. formats hgvsp for synonymous variants.
        major_consequence: set to most severe consequence for that transcript (
            VEP sometimes provides multiple consequences for a single transcript)
        major_consequence_rank: major_consequence rank based on VEP SO ontology (most severe = 1)
            (see http://www.ensembl.org/info/genome/variation/predicted_data.html)
        category: set to one of: "lof", "missense", "synonymous", "other" based on the value of major_consequence.

    Args:
        vep_root (StructExpression): root path of the VEP struct in the MT
        include_coding_annotations (bool): if True, fields relevant to protein-coding variants will be included
    """

    selected_annotations = [
        "biotype",
        "canonical",
        "cdna_start",
        "cdna_end",
        "codons",
        "gene_id",
        "gene_symbol",
        "hgvsc",
        "hgvsp",
        "transcript_id",
    ]

    if include_coding_annotations:
        selected_annotations.extend(
            [
                "amino_acids",
                "lof",
                "lof_filter",
                "lof_flags",
                "lof_info",
                "polyphen_prediction",
                "protein_id",
                "protein_start",
                "sift_prediction",
            ]
        )

    omit_consequence_terms = hl.set(omit_consequences) if omit_consequences else hl.empty_set(hl.tstr)

    result = hl.sorted(
        vep_root.transcript_consequences.map(
            lambda c: c.select(
                *selected_annotations,
                consequence_terms=c.consequence_terms.filter(lambda t: ~omit_consequence_terms.contains(t)),
                domains=c.domains.map(lambda domain: domain.db + ":" + domain.name),
                major_consequence=hl.cond(
                    c.consequence_terms.size() > 0,
                    hl.sorted(c.consequence_terms, key=lambda t: CONSEQUENCE_TERM_RANK_LOOKUP.get(t))[0],
                    hl.null(hl.tstr),
                )
            )
        )
        .filter(lambda c: c.consequence_terms.size() > 0)
        .map(
            lambda c: c.annotate(
                category=(
                    hl.case()
                    .when(
                        CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence)
                        <= CONSEQUENCE_TERM_RANK_LOOKUP.get("frameshift_variant"),
                        "lof",
                    )
                    .when(
                        CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence)
                        <= CONSEQUENCE_TERM_RANK_LOOKUP.get("missense_variant"),
                        "missense",
                    )
                    .when(
                        CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence)
                        <= CONSEQUENCE_TERM_RANK_LOOKUP.get("synonymous_variant"),
                        "synonymous",
                    )
                    .default("other")
                ),
                hgvs=get_expr_for_formatted_hgvs(c),
                major_consequence_rank=CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence),
            )
        ),
        lambda c: (
            hl.bind(
                lambda is_coding, is_most_severe, is_canonical: (
                    hl.cond(
                        is_coding,
                        hl.cond(is_most_severe, hl.cond(is_canonical, 1, 2), hl.cond(is_canonical, 3, 4)),
                        hl.cond(is_most_severe, hl.cond(is_canonical, 5, 6), hl.cond(is_canonical, 7, 8)),
                    )
                ),
                hl.or_else(c.biotype, "") == "protein_coding",
                hl.set(c.consequence_terms).contains(vep_root.most_severe_consequence),
                hl.or_else(c.canonical, 0) == 1,
            )
        ),
    )

    if not include_coding_annotations:
        # for non-coding variants, drop fields here that are hard to exclude in the above code
        result = result.map(lambda c: c.drop("domains", "hgvsp"))

    return hl.zip_with_index(result).map(
        lambda csq_with_index: csq_with_index[1].annotate(transcript_rank=csq_with_index[0])
    )


def get_expr_for_vep_protein_domains_set_from_sorted(vep_sorted_transcript_consequences_root):
    return hl.set(
        vep_sorted_transcript_consequences_root.flatmap(lambda c: c.domains)
    )


def get_expr_for_vep_gene_id_to_consequence_map(vep_sorted_transcript_consequences_root, gene_ids):
    # Manually build string because hl.json encodes a dictionary as [{ key: ..., value: ... }, ...]
    return (
        "{"
        + hl.delimit(
            gene_ids.map(
                lambda gene_id: hl.bind(
                    lambda worst_consequence_in_gene: '"' + gene_id + '":"' + worst_consequence_in_gene.major_consequence + '"',
                    vep_sorted_transcript_consequences_root.find(lambda c: c.gene_id == gene_id)
                )
            )
        )
        + "}"
    )


def get_expr_for_vep_transcript_id_to_consequence_map(vep_transcript_consequences_root):
    # Manually build string because hl.json encodes a dictionary as [{ key: ..., value: ... }, ...]
    return (
        "{"
        + hl.delimit(
            vep_transcript_consequences_root.map(lambda c: '"' + c.transcript_id + '": "' + c.major_consequence + '"')
        )
        + "}"
    )


def get_expr_for_vep_transcript_ids_set(vep_transcript_consequences_root):
    return hl.set(vep_transcript_consequences_root.map(lambda c: c.transcript_id))


def get_expr_for_worst_transcript_consequence_annotations_struct(
    vep_sorted_transcript_consequences_root, include_coding_annotations=True
):
    """Retrieves the top-ranked transcript annotation based on the ranking computed by
    get_expr_for_vep_sorted_transcript_consequences_array(..)

    Args:
        vep_sorted_transcript_consequences_root (ArrayExpression):
        include_coding_annotations (bool):
    """

    transcript_consequences = {
        "biotype": hl.tstr,
        "canonical": hl.tint,
        "category": hl.tstr,
        "cdna_start": hl.tint,
        "cdna_end": hl.tint,
        "codons": hl.tstr,
        "gene_id": hl.tstr,
        "gene_symbol": hl.tstr,
        "hgvs": hl.tstr,
        "hgvsc": hl.tstr,
        "major_consequence": hl.tstr,
        "major_consequence_rank": hl.tint,
        "transcript_id": hl.tstr,
    }

    if include_coding_annotations:
        transcript_consequences.update(
            {
                "amino_acids": hl.tstr,
                "domains": hl.tstr,
                "hgvsp": hl.tstr,
                "lof": hl.tstr,
                "lof_flags": hl.tstr,
                "lof_filter": hl.tstr,
                "lof_info": hl.tstr,
                "polyphen_prediction": hl.tstr,
                "protein_id": hl.tstr,
                "sift_prediction": hl.tstr,
            }
        )

    return hl.cond(
        vep_sorted_transcript_consequences_root.size() == 0,
        hl.struct(**{field: hl.null(field_type) for field, field_type in transcript_consequences.items()}),
        hl.bind(
            lambda worst_transcript_consequence: (
                worst_transcript_consequence.annotate(
                    domains=hl.delimit(hl.set(worst_transcript_consequence.domains))
                ).select(*transcript_consequences.keys())
            ),
            vep_sorted_transcript_consequences_root[0],
        ),
    )
