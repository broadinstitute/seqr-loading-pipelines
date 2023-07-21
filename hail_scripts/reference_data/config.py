import hail as hl

from hail_scripts.reference_data.clinvar import (
    CLINVAR_ASSERTIONS,
    CLINVAR_DEFAULT_PATHOGENICITY,
    CLINVAR_GOLD_STARS_LOOKUP,
    CLINVAR_PATHOGENICITIES,
    CLINVAR_PATHOGENICITIES_LOOKUP,
    download_and_import_latest_clinvar_vcf,
    parsed_and_mapped_clnsigconf,
    parsed_clnsig,
)
from hail_scripts.reference_data.hgmd import download_and_import_hgmd_vcf

from v03_pipeline.lib.model import ReferenceDatasetCollection


def import_locus_intervals(
    url: str,
    genome_version: str,
) -> hl.Table:
    return hl.import_locus_intervals(url, f'GRCh{genome_version}')


def predictor_parse(field: hl.StringExpression):
    return field.split(';').find(lambda p: p != '.')


def clinvar_custom_select(ht):
    selects = {}
    clnsigs = parsed_clnsig(ht)
    selects['pathogenicity'] = hl.if_else(
        CLINVAR_PATHOGENICITIES_LOOKUP.contains(clnsigs[0]),
        clnsigs[0],
        CLINVAR_DEFAULT_PATHOGENICITY,
    )
    selects['assertion'] = hl.if_else(
        CLINVAR_PATHOGENICITIES_LOOKUP.contains(clnsigs[0]),
        clnsigs[1:],
        clnsigs,
    )
    # NB: the `enum_select` does not support mapping a list of tuples
    # so there's a hidden enum-mapping inside this clinvar function.
    selects['conflictingPathogenicities'] = parsed_and_mapped_clnsigconf(ht)
    selects['goldStars'] = CLINVAR_GOLD_STARS_LOOKUP.get(hl.delimit(ht.info.CLNREVSTAT))
    return selects


def dbnsfp_custom_select(ht):
    selects = {}
    selects['REVEL_score'] = hl.parse_float32(ht.REVEL_score)
    selects['SIFT_pred'] = predictor_parse(ht.SIFT_pred)
    selects['Polyphen2_HVAR_pred'] = predictor_parse(ht.Polyphen2_HVAR_pred)
    selects['MutationTaster_pred'] = predictor_parse(ht.MutationTaster_pred)
    return selects


def dbnsfp_custom_select_38(ht):
    selects = dbnsfp_custom_select(ht)
    selects['VEST4_score'] = hl.parse_float32(predictor_parse(ht.VEST4_score))
    selects['MutPred_score'] = hl.parse_float32(ht.MutPred_score)
    return selects


def dbnsfp_filter(
    ht: hl.Table,
    reference_dataset_collection: ReferenceDatasetCollection,
) -> hl.BooleanExpression:
    return (
        reference_dataset_collection != ReferenceDatasetCollection.COMBINED_MITO
    ) | (ht.locus.contig == 'chrM')


def custom_gnomad_select_v2(ht):
    """
    Custom select for public gnomad v2 dataset (which we did not generate). Extracts fields like
    'AF', 'AN', and generates 'hemi'.
    :param ht: hail table
    :return: select expression dict
    """
    selects = {}
    global_idx = hl.eval(ht.globals.freq_index_dict['gnomad'])
    selects['AF'] = hl.float32(ht.freq[global_idx].AF)
    selects['AN'] = ht.freq[global_idx].AN
    selects['AC'] = ht.freq[global_idx].AC
    selects['Hom'] = ht.freq[global_idx].homozygote_count

    selects['AF_POPMAX_OR_GLOBAL'] = hl.float32(
        hl.or_else(
            ht.popmax[ht.globals.popmax_index_dict['gnomad']].AF,
            ht.freq[global_idx].AF,
        ),
    )
    selects['FAF_AF'] = hl.float32(ht.faf[ht.globals.popmax_index_dict['gnomad']].faf95)
    selects['Hemi'] = hl.if_else(
        ht.locus.in_autosome_or_par(),
        0,
        ht.freq[ht.globals.freq_index_dict['gnomad_male']].AC,
    )
    return selects


def custom_gnomad_select_v3(ht):
    """
    Custom select for public gnomad v3 dataset (which we did not generate). Extracts fields like
    'AF', 'AN', and generates 'hemi'.
    :param ht: hail table
    :return: select expression dict
    """
    selects = {}
    global_idx = hl.eval(ht.globals.freq_index_dict['adj'])
    selects['AF'] = hl.float32(ht.freq[global_idx].AF)
    selects['AN'] = ht.freq[global_idx].AN
    selects['AC'] = ht.freq[global_idx].AC
    selects['Hom'] = ht.freq[global_idx].homozygote_count

    selects['AF_POPMAX_OR_GLOBAL'] = hl.float32(
        hl.or_else(ht.popmax.AF, ht.freq[global_idx].AF),
    )
    selects['FAF_AF'] = hl.float32(ht.faf[ht.globals.faf_index_dict['adj']].faf95)
    selects['Hemi'] = hl.if_else(
        ht.locus.in_autosome_or_par(),
        0,
        ht.freq[ht.globals.freq_index_dict['XY-adj']].AC,
    )
    return selects


def custom_mpc_select(ht):
    selects = {}
    selects['MPC'] = hl.parse_float32(ht.info.MPC)
    return selects


"""
Configurations of dataset to combine.
Format:
'<Name of dataset>': {
    '<Reference genome version>': {
        'path': 'gs://path/to/hailtable.ht',
        'select': '<Optional list of fields to select or dict of new field name to location of old field
            in the reference dataset. If '#' is at the end, we know to select the appropriate biallelic
            using the a_index.>',
        'custom_select': '<Optional function of custom select function>',
        'enum_select': '<Optional dictionary mapping field_name to a list of enumerated values.>'
    },
"""
CONFIG = {
    'cadd': {
        '37': {
            'version': 'v1.6',
            'path': 'gs://seqr-reference-data/GRCh37/CADD/CADD_snvs_and_indels.v1.6.ht',
            'select': ['PHRED'],
        },
        '38': {
            'version': 'v1.6',
            'path': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
            'select': ['PHRED'],
        },
    },
    'clinvar': {
        '37': {
            'custom_import': download_and_import_latest_clinvar_vcf,
            'source_path': 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar.vcf.gz',
            'select': {'alleleId': 'info.ALLELEID'},
            'custom_select': clinvar_custom_select,
            'enum_select': {
                'pathogenicity': CLINVAR_PATHOGENICITIES,
                'assertion': CLINVAR_ASSERTIONS,
            },
        },
        '38': {
            'custom_import': download_and_import_latest_clinvar_vcf,
            'source_path': 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz',
            'select': {'alleleId': 'info.ALLELEID'},
            'custom_select': clinvar_custom_select,
            'enum_select': {
                'pathogenicity': CLINVAR_PATHOGENICITIES,
                'assertion': CLINVAR_ASSERTIONS,
            },
        },
    },
    'dbnsfp': {
        '37': {
            'version': '2.9.3',
            'path': 'gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.ht',
            'custom_select': dbnsfp_custom_select,
            'enum_select': {
                'SIFT_pred': ['D', 'T'],
                'Polyphen2_HVAR_pred': ['D', 'P', 'B'],
                'MutationTaster_pred': ['D', 'A', 'N', 'P'],
            },
        },
        '38': {
            'version': '4.2',
            'path': 'gs://seqr-reference-data/GRCh38/dbNSFP/v4.2/dbNSFP4.2a_variant.ht',
            'select': [
                'fathmm_MKL_coding_pred',
            ],
            'custom_select': dbnsfp_custom_select_38,
            'enum_select': {
                'SIFT_pred': ['D', 'T'],
                'Polyphen2_HVAR_pred': ['D', 'P', 'B'],
                'MutationTaster_pred': ['D', 'A', 'N', 'P'],
                'fathmm_MKL_coding_pred': ['D', 'N'],
            },
            'filter': dbnsfp_filter,
        },
    },
    'eigen': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/eigen/EIGEN_coding_noncoding.grch37.ht',
            'select': {'Eigen_phred': 'info.Eigen-phred'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/eigen/EIGEN_coding_noncoding.liftover_grch38.ht',
            'select': {'Eigen_phred': 'info.Eigen-phred'},
        },
    },
    'hgmd': {
        '37': {
            'custom_import': download_and_import_hgmd_vcf,
            'source_path': 'gs://seqr-reference-data-private/GRCh37/HGMD/HGMD_Pro_2023.1_hg19.vcf.gz',
            'select': {'accession': 'rsid', 'class': 'info.CLASS'},
            'enum_select': {
                'class': [
                    'DM',
                    'DM?',
                    'DP',
                    'DFP',
                    'FP',
                    'R',
                ],
            },
        },
        '38': {
            'custom_import': download_and_import_hgmd_vcf,
            'source_path': 'gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_Pro_2023.1_hg38.vcf.gz',
            'select': {'accession': 'rsid', 'class': 'info.CLASS'},
            'enum_select': {
                'class': [
                    'DM',
                    'DM?',
                    'DP',
                    'DFP',
                    'FP',
                    'R',
                ],
            },
        },
    },
    'mpc': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.ht',
            'custom_select': custom_mpc_select,
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/MPC/fordist_constraint_official_mpc_values.liftover.GRCh38.ht',
            'custom_select': custom_mpc_select,
        },
    },
    'primate_ai': {
        '37': {
            'version': 'v0.2',
            'path': 'gs://seqr-reference-data/GRCh37/primate_ai/PrimateAI_scores_v0.2.ht',
            'select': {'score': 'info.score'},
        },
        '38': {
            'version': 'v0.2',
            'path': 'gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
            'select': {'score': 'info.score'},
        },
    },
    'splice_ai': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/spliceai/spliceai_scores.ht',
            'select': {
                'delta_score': 'info.max_DS',
                'splice_consequence': 'info.splice_consequence',
            },
            'enum_select': {
                'splice_consequence': [
                    'Acceptor gain',
                    'Acceptor loss',
                    'Donor gain',
                    'Donor loss',
                    'No consequence',
                ],
            },
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/spliceai/spliceai_scores.ht',
            'select': {
                'delta_score': 'info.max_DS',
                'splice_consequence': 'info.splice_consequence',
            },
            'enum_select': {
                'splice_consequence': [
                    'Acceptor gain',
                    'Acceptor loss',
                    'Donor gain',
                    'Donor loss',
                    'No consequence',
                ],
            },
        },
    },
    'topmed': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.ht',
            'select': {
                'AC': 'info.AC#',
                'AF': 'info.AF#',
                'AN': 'info.AN',
                'Hom': 'info.Hom#',
                'Het': 'info.Het#',
            },
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/TopMed/freeze8/TOPMed.all.ht',
            'select': {
                'AC': 'info.AC',
                'AF': 'info.AF',
                'AN': 'info.AN',
                'Hom': 'info.Hom',
                'Het': 'info.Het',
            },
        },
    },
    'gnomad_exomes': {
        '37': {
            'version': 'r2.1.1',
            'path': 'gs://gcp-public-data--gnomad/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
            'custom_select': custom_gnomad_select_v2,
        },
        '38': {
            'version': 'r2.1.1',
            'path': 'gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/exomes/gnomad.exomes.r2.1.1.sites.liftover_grch38.ht',
            'custom_select': custom_gnomad_select_v2,
        },
    },
    'gnomad_genomes': {
        '37': {
            'version': 'r2.1.1',
            'path': 'gs://gcp-public-data--gnomad/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
            'custom_select': custom_gnomad_select_v2,
        },
        '38': {
            'version': 'v3.1.2',
            'path': 'gs://gcp-public-data--gnomad/release/3.1.2/ht/genomes/gnomad.genomes.v3.1.2.sites.ht',
            'custom_select': custom_gnomad_select_v3,
        },
    },
    'exac': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/gnomad/ExAC.r1.sites.vep.ht',
            'select': {
                'AF_POPMAX': 'info.AF_POPMAX',
                'AF': 'info.AF#',
                'AC_Adj': 'info.AC_Adj#',
                'AC_Het': 'info.AC_Het#',
                'AC_Hom': 'info.AC_Hom#',
                'AC_Hemi': 'info.AC_Hemi#',
                'AN_Adj': 'info.AN_Adj',
            },
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/gnomad/ExAC.r1.sites.liftover.b38.ht',
            'select': {
                'AF_POPMAX': 'info.AF_POPMAX',
                'AF': 'info.AF#',
                'AC_Adj': 'info.AC_Adj#',
                'AC_Het': 'info.AC_Het#',
                'AC_Hom': 'info.AC_Hom#',
                'AC_Hemi': 'info.AC_Hemi#',
                'AN_Adj': 'info.AN_Adj',
            },
        },
    },
    'gnomad_non_coding_constraint': {
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/gnomad_nc_constraint/gnomad_non-coding_constraint_z_scores.ht',
            'select': {'z_score': 'target'},
        },
    },
    'screen': {
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/ccREs/GRCh38-ccREs.ht',
            'select': {'region_type': 'target'},
            'enum_select': {
                'region_type': [
                    'CTCF-bound',
                    'CTCF-only',
                    'DNase-H3K4me3',
                    'PLS',
                    'dELS',
                    'pELS',
                    'DNase-only',
                    'low-DNase',
                ],
            },
        },
    },
    'gnomad_mito': {
        '38': {
            'version': 'v3.1',
            'path': 'gs://gcp-public-data--gnomad/release/3.1/ht/genomes/gnomad.genomes.v3.1.sites.chrM.ht',
            'select': {
                'AN': 'AN',
                'AC': 'AC_hom',
                'AC_het': 'AC_het',
                'AF': 'AF_hom',
                'AF_het': 'AF_het',
                'max_hl': 'max_hl',
            },
        },
    },
    'mitomap': {
        '38': {
            'version': 'Feb. 04 2022',
            'path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/Mitomap Confirmed Mutations Feb. 04 2022.ht',
            'select': ['pathogenic'],
        },
    },
    'mitimpact': {
        '38': {
            'version': '3.0.7',
            'path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MitImpact/MitImpact_db_3.0.7.ht',
            'select': {'score': 'APOGEE_score'},
        },
    },
    'hmtvar': {
        '38': {
            'version': 'Jan. 10 2022',
            'path': 'gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar%20Jan.%2010%202022.ht',
            'select': {'score': 'disease_score'},
        },
    },
    'helix_mito': {
        '38': {
            'version': '20200327',
            'path': 'gs://seqr-reference-data/GRCh38/mitochondrial/Helix/HelixMTdb_20200327.ht',
            'select': {
                'AC': 'counts_hom',
                'AF': 'AF_hom',
                'AC_het': 'counts_het',
                'AF_het': 'AF_het',
                'AN': 'AN',
                'max_hl': 'max_ARF',
            },
        },
    },
    'high_constraint_region_mito': {
        '38': {
            'version': 'Feb-15-2022',
            'source_path': 'gs://seqr-reference-data/GRCh38/mitochondrial/Helix high constraint intervals Feb-15-2022.tsv',
            'custom_import': import_locus_intervals,
        },
    },
}
