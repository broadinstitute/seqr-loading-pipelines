from copy import deepcopy

import hail as hl


def custom_gnomad_select_v2(ht):
    """
    Custom select for public gnomad v2 dataset (which we did not generate). Extracts fields like
    'AF', 'AN', and generates 'hemi'.
    :param ht: hail table
    :return: select expression dict
    """
    selects = {}
    global_idx = hl.eval(ht.globals.freq_index_dict['gnomad'])
    selects['AF'] = ht.freq[global_idx].AF
    selects['AN'] = ht.freq[global_idx].AN
    selects['AC'] = ht.freq[global_idx].AC
    selects['Hom'] = ht.freq[global_idx].homozygote_count

    selects['AF_POPMAX_OR_GLOBAL'] = hl.or_else(
        ht.popmax[ht.globals.popmax_index_dict['gnomad']].AF, ht.freq[global_idx].AF,
    )
    selects['FAF_AF'] = ht.faf[ht.globals.popmax_index_dict['gnomad']].faf95
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
    selects['AF'] = ht.freq[global_idx].AF
    selects['AN'] = ht.freq[global_idx].AN
    selects['AC'] = ht.freq[global_idx].AC
    selects['Hom'] = ht.freq[global_idx].homozygote_count

    selects['AF_POPMAX_OR_GLOBAL'] = hl.or_else(ht.popmax.AF, ht.freq[global_idx].AF)
    selects['FAF_AF'] = ht.faf[ht.globals.faf_index_dict['adj']].faf95
    selects['Hemi'] = hl.if_else(
        ht.locus.in_autosome_or_par(),
        0,
        ht.freq[ht.globals.freq_index_dict['XY-adj']].AC,
    )
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
        'field_name': '<Optional name of root annotation in combined dataset, defaults to name of dataset.>',
        'custom_select': '<Optional function of custom select function>',
        'enum_select': '<Optional dictionary mapping field_name to a list of enumerated values.>'
    },
"""
CONFIG = {
    '1kg': {  # tgp
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/1kg/1kg.wgs.phase3.20130502.GRCh37_sites.ht',
            'select': {
                'AC': 'info.AC#',
                'AF': 'info.AF#',
                'AN': 'info.AN',
                'POPMAX_AF': 'POPMAX_AF',
            },
            'field_name': 'g1k',
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/1kg/1kg.wgs.phase3.20170504.GRCh38_sites.ht',
            'select': {
                'AC': 'info.AC#',
                'AF': 'info.AF#',
                'AN': 'info.AN',
                'POPMAX_AF': 'POPMAX_AF',
            },
            'field_name': 'g1k',
        },
    },
    'cadd': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/CADD/CADD_snvs_and_indels.v1.6.ht',
            'select': ['PHRED'],
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.6.ht',
            'select': ['PHRED'],
        },
    },
    'dbnsfp': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.ht',
            'select': [
                'SIFT_pred',
                'Polyphen2_HVAR_pred',
                'MutationTaster_pred',
                'FATHMM_pred',
                'MetaSVM_pred',
                'REVEL_score',
                'GERP_RS',
                'phastCons100way_vertebrate',
            ],
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/dbNSFP/v4.2/dbNSFP4.2a_variant.ht',
            'select': [
                'SIFT_pred',
                'Polyphen2_HVAR_pred',
                'MutationTaster_pred',
                'FATHMM_pred',
                'MetaSVM_pred',
                'REVEL_score',
                'GERP_RS',
                'phastCons100way_vertebrate',
                'VEST4_score',
                'fathmm_MKL_coding_pred',
                'MutPred_score',
            ],
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
            'path': 'gs://seqr-reference-data-private/GRCh37/HGMD/HGMD_Pro_2022.4_hg19.vcf.gz',
            'select': {'accession': 'rsid'},
            'enum_selects': [{
                'src': 'info.CLASS',
                'dst': 'class_id',
                'values': [
                    'DFP',
                    'DM',
                    'DM?',
                    'DP', 
                    'FP',
                    'R',
                ],
            }],
        },
        '38': {
            'path': 'gs://seqr-reference-data-private/GRCh38/HGMD/HGMD_Pro_2022.4_hg38.vcf.gz',
            'select': {'accession': 'rsid'},
            'enum_selects': [{
                'src': 'info.CLASS',
                'dst': 'class_id',
                'values': [
                    'DFP',
                    'DM',
                    'DM?',
                    'DP', 
                    'FP',
                    'R',
                ],
            }],
        },
    },
    'mpc': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.ht',
            'select': {'MPC': 'info.MPC'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/MPC/fordist_constraint_official_mpc_values.liftover.GRCh38.ht',
            'select': {'MPC': 'info.MPC'},
        },
    },
    'primate_ai': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/primate_ai/PrimateAI_scores_v0.2.ht',
            'select': {'score': 'info.score'},
        },
        '38': {
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
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/spliceai/spliceai_scores.ht',
            'select': {
                'delta_score': 'info.max_DS',
                'splice_consequence': 'info.splice_consequence',
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
    'gnomad_exome_coverage': {
        '37': {
            'path': 'gs://gcp-public-data--gnomad/release/2.1/coverage/exomes/gnomad.exomes.r2.1.coverage.ht',
            'select': {'x10': '10'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/gnomad_coverage/GRCh38/exomes/gnomad.exomes.r2.1.coverage.liftover_grch38.ht',
            'select': {'x10': 'over_10'},
        },
    },
    'gnomad_genome_coverage': {
        '37': {
            'path': 'gs://gcp-public-data--gnomad/release/2.1/coverage/genomes/gnomad.genomes.r2.1.coverage.ht',
            'select': {'x10': '10'},
        },
        '38': {
            'path': 'gs://gcp-public-data--gnomad/release/3.0/coverage/genomes/gnomad.genomes.r3.0.coverage.ht/',
            'select': {'x10': 'over_10'},
        },
    },
    'gnomad_exomes': {
        '37': {
            'path': 'gs://gcp-public-data--gnomad/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
            'custom_select': custom_gnomad_select_v2,
        },
        '38': {
            'path': 'gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/exomes/gnomad.exomes.r2.1.1.sites.liftover_grch38.ht',
            'custom_select': custom_gnomad_select_v2,
        },
    },
    'gnomad_genomes': {
        '37': {
            'path': 'gs://gcp-public-data--gnomad/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
            'custom_select': custom_gnomad_select_v2,
        },
        '38': {
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
            'path' : 'gs://seqr-reference-data/GRCh38/ccREs/GRCh38-ccREs.ht',
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
    'geno2mp': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/geno2mp/Geno2MP.variants.ht',
            'select': {'HPO_Count': 'info.HPO_CT'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/geno2mp/Geno2MP.variants.liftover_38.ht',
            'select': {'HPO_Count': 'info.HPO_CT'},
        },
    },
    'gnomad_mito': {
        '38': {
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
            'path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MITOMAP/Mitomap Confirmed Mutations Feb. 04 2022.ht',
            'select': ['pathogenic'],
        },
    },
    'mitimpact': {
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/mitochondrial/MitImpact/MitImpact_db_3.0.7.ht',
            'select': {'score': 'APOGEE_score'},
        },
    },
    'hmtvar': {
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/mitochondrial/HmtVar/HmtVar%20Jan.%2010%202022.ht',
            'select': {'score': 'disease_score'},
        },
    },
    'helix_mito': {
        '38': {
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
}

CONFIG['dbnsfp_mito'] = {'38': deepcopy(CONFIG['dbnsfp']['38'])}
CONFIG['dbnsfp_mito']['38']['filter'] = lambda ht: ht.locus.contig == 'chrM'

GCS_PREFIXES = {
    ('dev', 'public'): 'gs://seqr-scratch-temp/GRCh{genome_version}/v03',
    ('dev', 'private'): 'gs://seqr-scratch-temp/GRCh{genome_version}/v03',
    ('prod', 'public'): 'gs://seqr-reference-data/GRCh{genome_version}/v03',
    ('prod', 'private'): 'gs://seqr-reference-data-private/GRCh{genome_version}/v03',
}
