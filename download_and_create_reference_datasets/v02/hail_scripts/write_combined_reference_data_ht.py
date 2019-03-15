from datetime import datetime
from functools import reduce
import os

import hail as hl

OUTPUT_DIR = 'gs://seqr-kev/combined-test'


CONFIG =  {
    '1kg': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/1kg/1kg.wgs.phase3.20130502.GRCh37_sites.ht',
            'select': {'AC': 'info.AC#', 'AF': 'info.AF#', 'AN': 'info.AN', 'POPMAX_AF': 'POPMAX_AF'},
            'field_name': 'g1k',
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/1kg/1kg.wgs.phase3.20170504.GRCh38_sites.ht',
            'select': {'AC': 'info.AC#', 'AF': 'info.AF#', 'AN': 'info.AN', 'POPMAX_AF': 'POPMAX_AF'},
            'field_name': 'g1k',
        },
    },
    'cadd': {
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/CADD/CADD_snvs_and_indels.v1.4.ht',
            'select': ['PHRED'],
        },
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/CADD/CADD_snvs_and_indels.v1.4.ht',
            'select': ['PHRED'],
        },
    },
    'dbnsfp': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/dbNSFP/v2.9.3/dbNSFP2.9.3_variant.ht',
            'select': ['SIFT_pred', 'Polyphen2_HVAR_pred', 'MutationTaster_pred', 'FATHMM_pred', 'MetaSVM_pred', 'REVEL_score',
                          'GERP_RS', 'phastCons100way_vertebrate'],
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/dbNSFP/v3.5/dbNSFP3.5a_variant.ht',
            'select': ['SIFT_pred', 'Polyphen2_HVAR_pred', 'MutationTaster_pred', 'FATHMM_pred', 'MetaSVM_pred', 'REVEL_score',
                          'GERP_RS', 'phastCons100way_vertebrate'],
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
            'select': {'delta_score': 'info.max_DS'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/spliceai/spliceai_scores.ht',
            'select': {'delta_score': 'info.max_DS'},
        },
    },
    'topmed': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/TopMed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.ht',
            'select': {'AC': 'info.AC#', 'AF': 'info.AF#', 'AN': 'info.AN', 'Hom': 'info.Hom#', 'Het': 'info.Het#'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/TopMed/bravo-dbsnp-all.ht',
            'select': {'AC': 'info.AC#', 'AF': 'info.AF#', 'AN': 'info.AN', 'Hom': 'info.Hom#', 'Het': 'info.Het#'},
        },
    },
    'gnomad_exome_coverage': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1/coverage/exomes/gnomad.exomes.r2.1.coverage.ht',
        },
    },
    'gnomad_genome_coverage': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1/coverage/genomes/gnomad.genomes.r2.1.coverage.ht',
            'select': {'AC': 'info.AC'},
        },
    },
    'gnomad_exomes': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
            'custom_select': 'custom_gnomad_select'
        },
    },
    'gnomad_genomes': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
            'custom_select': 'custom_gnomad_select'
        },
    },
    'exac': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/gnomad/ExAC.r1.sites.vep.ht',
            'select': {'AF_POPMAX': 'info.AF_POPMAX', 'AF': 'info.AF#', 'AC_Adj': 'info.AC_Adj#', 'AC_Het': 'info.AC_Het#',
                       'AC_Hom': 'info.AC_Hom#', 'AC_Hemi': 'info.AC_Hemi#', 'AN_Adj': 'info.AN_Adj',},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/gnomad/ExAC.r1.sites.liftover.b38.htt',
        },
    },

}

def annotate_coverages(ht, coverage_dataset, reference_genome):
    coverage_ht = hl.read_table(CONFIG[coverage_dataset][reference_genome]['path'])
    return ht.annotate(**{coverage_dataset: coverage_ht[ht.locus].over_10})

def custom_gnomad_select(ht):
    selects = {}
    global_idx = hl.eval(ht.globals.freq_index_dict['gnomad'])
    selects['AF'] = ht.freq[global_idx].AF
    selects['AN'] = ht.freq[global_idx].AN
    selects['AC'] = ht.freq[global_idx].AC
    selects['hom'] = ht.freq[global_idx].homozygote_count

    selects['POPMAX_AF'] = ht.popmax[ht.globals.popmax_index_dict['gnomad']].AF
    selects['FAF_AF'] = ht.faf[ht.globals.popmax_index_dict['gnomad']].faf95
    selects['hemi'] = hl.cond(ht.locus.in_autosome_or_par(),
                              0, ht.freq[ht.globals.freq_index_dict['gnomad_male']].AC)
    return selects

def get_select_fields(config, base_ht):
    select_fields = {}
    selects = config.get('select') or None
    if selects is not None:
        if isinstance(selects, list):
            select_fields = { selection: base_ht[selection] for selection in selects }
        elif isinstance(selects, dict):
            for key, val in selects.items():
                ht = base_ht
                for attr in val.split('.'):
                    if attr.endswith('#'):
                        attr = attr[:-1]
                        ht = ht[attr][base_ht.a_index-1]
                    else:
                        ht = ht[attr]
                select_fields[key] = ht
    return select_fields

def get_ht(dataset, reference_genome):
    config = CONFIG[dataset][reference_genome]

    base_ht = hl.read_table(config['path'])

    select_fields = get_select_fields(config, base_ht)

    if 'custom_select' in config:
        custom_select_fn_str = config['custom_select']
        select_fields = {**select_fields, **globals()[custom_select_fn_str](base_ht)}

    print(select_fields)

    field_name = config.get('field_name') or dataset
    select_query = {
        field_name: hl.struct(**select_fields)
    }

    return base_ht.select(**select_query).distinct()

def join_hts(datasets, coverage_datasets=None, reference_genome='37'):
    hts = [get_ht(dataset, reference_genome) for dataset in datasets]
    joined_ht = reduce((lambda joined_ht, ht: joined_ht.join(ht, 'outer')), hts)

    for coverage_dataset in coverage_datasets:
        joined_ht = annotate_coverages(joined_ht, coverage_dataset, reference_genome)

    joined_ht = joined_ht.select_globals(date=datetime.now().isoformat())
    joined_ht.describe()
    print(os.path.join(OUTPUT_DIR, 'combined--%s.ht' % '-'.join(datasets + coverage_datasets)))

    joined_ht.write(os.path.join(OUTPUT_DIR, 'combined--%s.ht' % '-'.join(datasets + coverage_datasets)))

def run():
    # join_hts(['gnomad_genomes', 'exac'], ['gnomad_genome_coverage', 'gnomad_exome_coverage'], '37')
    join_hts(['1kg', 'mpc', 'cadd', 'eigen', 'dbnsfp', 'topmed', 'primate_ai', 'splice_ai', 'exac',
              'gnomad_genomes', 'gnomad_exomes'],
             ['gnomad_genome_coverage', 'gnomad_exome_coverage'],
            '37')
    # join_hts(['1kg', 'mpc', 'cadd', 'eigen', 'dbnsfp', 'topmed', 'primate_ai', 'splice_ai'],
    #          '38')

run()