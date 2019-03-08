import os

import hail as hl

OUTPUT_DIR = 'gs://seqr-kev/combined-test'

CONFIG =  {
    '1kg': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/1kg/1kg.wgs.phase3.20130502.GRCh37_sites.ht',
            'selection': {'AC': 'info.AC', 'AF': 'info.AF', 'AN': 'info.AN', 'POPMAX_AF': 'POPMAX_AF'},
            'field_name': 'g1k',
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/1kg/1kg.wgs.phase3.20170504.GRCh38_sites.ht',
            'selection': {'AC': 'info.AC', 'AF': 'info.AF', 'AN': 'info.AN', 'POPMAX_AF': 'POPMAX_AF'},
            'field_name': 'g1k',
        },
    },
    'cadd': {
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.4.ht',
            'selection': ['PHRED'],
        },
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/CADD/CADD_snvs_and_indels.v1.4.ht',
            'selection': ['PHRED'],
        },
    },
    'dbnsfp': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.ht',
            'selection': ['SIFT_pred', 'Polyphen2_HVAR_pred', 'MutationTaster_pred', 'FATHMM_pred', 'MetaSVM_pred', 'REVEL_score',
                          'GERP_RS', 'phastCons100way_vertebrate'],
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/dbNSFP/v3.5/dbNSFP3.5a_variant.ht',
            'selection': ['SIFT_pred', 'Polyphen2_HVAR_pred', 'MutationTaster_pred', 'FATHMM_pred', 'MetaSVM_pred', 'REVEL_score',
                          'GERP_RS', 'phastCons100way_vertebrate'],
        },
    },
    'eigen': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/eigen/EIGEN_coding_noncoding.grch37.ht',
            'selection': {'Eigen_phred': 'info.Eigen-phred'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/eigen/EIGEN_coding_noncoding.liftover_grch38.ht',
            'selection': {'Eigen_phred': 'info.Eigen-phred'},
        },
    },
    'mpc': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/MPC/fordist_constraint_official_mpc_values.ht',
            'selection': {'MPC': 'info.MPC'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/MPC/fordist_constraint_official_mpc_values.liftover.GRCh38.ht',
            'selection': {'MPC': 'info.MPC'},
        },
    },
    'primate_ai': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/primate_ai/PrimateAI_scores_v0.2.ht',
            'selection': {'score': 'info.score'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/primate_ai/PrimateAI_scores_v0.2.liftover_grch38.ht',
            'selection': {'score': 'info.score'},
        },
    },
    'splice_ai': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/spliceai/spliceai_scores.ht',
            'selection': {'delta_score': 'info.max_DS'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/spliceai/spliceai_scores.ht',
            'selection': {'delta_score': 'info.max_DS'},
        },
    },
    'topmed': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.ht',
            'selection': {'AC': 'info.AC'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/TopMed/bravo-dbsnp-all.ht',
            'selection': {'AC': 'info.AC'},
        },
    },
    'gnomad_exome_coverage': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1/coverage/exomes/gnomad.exomes.r2.1.coverage.ht',
            'selection': {'AC': 'info.AC'},
        },
    },
    'gnomad_genome_coverage': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1/coverage/genomes/gnomad.genomes.r2.1.coverage.ht',
            'selection': {'AC': 'info.AC'},
        },
    },
    'gnomad_exomes': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
            'selection': {'AC': 'info.AC'},
        },
    },
    'gnomad_genome': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
            'selection': {'AC': 'info.AC'},
        },
    },
    'exac': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.ht',
            'selection': {'AC': 'info.AC'},
        },
    },

}

def get_mt(dataset, reference_genome):
    config = CONFIG[dataset][reference_genome]

    base_ht = hl.read_table(config['path'])

    field_name = config.get('field_name') or dataset
    if isinstance(config['selection'], list):
        select_fields = {selection: base_ht[selection] for selection in config['selection'] }
    elif isinstance(config['selection'], dict):
        select_fields = {}
        for key, val in config['selection'].items():
            ht = base_ht
            for attr in val.split('.'):
                ht = ht[attr]
            select_fields[key] = ht

    print(select_fields)
    select_query = {
        field_name: hl.struct(**select_fields)
    }

    return base_ht.select(**select_query)

def join_mts(datasets, reference_genome):
    joined_mt = None
    for dataset in datasets:
        dataset_mt = get_mt(dataset, reference_genome)
        if joined_mt == None:
            joined_mt = dataset_mt
            continue
        else:
            joined_mt = joined_mt.join(dataset_mt)
    joined_mt.describe()
    print(os.path.join(OUTPUT_DIR, 'combined-%s.ht' % '-'.join(datasets)))
    # joined_mt.write(os.path.join(OUTPUT_DIR, 'combined%s.ht' % combined_str))

def run():
    join_mts(['1kg', 'mpc', 'cadd', 'eigen', 'dbnsfp', 'topmed', 'primate_ai', 'splice_ai'],
             '37')
    join_mts(['1kg', 'mpc', 'cadd', 'eigen', 'dbnsfp', 'topmed', 'primate_ai', 'splice_ai'],
             '38')

run()