import argparse
from datetime import datetime
from functools import reduce
import logging
import os

import hail as hl


OUTPUT_TEMPLATE = 'gs://seqr-reference-data/GRCh{genome_version}/' \
                  'all_reference_data/combined_reference_data_grch{genome_version}.ht'

'''
Configurations of dataset to combine. 
Format:
'<Name of dataset>': {
    '<Reference genome version>': {
        'path': 'gs://path/to/hailtable.ht',
        'select': '<Optional list of fields to select or dict of new field name to location of old field
            in the reference dataset. If '#' is at the end, we know to select the appropriate biallelic
            using the a_index.>',
        'field_name': '<Optional name of root annotation in combined dataset, defaults to name of dataset.>',
        'custom_select': '<Optional function name of custom select function>',
    },
'''
CONFIG = {
    '1kg': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/1kg/1kg.wgs.phase3.20130502.GRCh37_sites.ht',
            'select': {'AC': 'info.AC#', 'AF': 'info.AF#', 'AN': 'info.AN', 'POPMAX_AF': 'POPMAX_AF'},
            'field_name': 'g1k',
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/1kg/1kg.wgs.phase3.20170504.GRCh38_sites_test.ht',
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
            'select': {'x10': '10'}
        },
        '38': {
            'path': 'gs://seqr-reference-data/gnomad_coverage/GRCh38/exomes/gnomad.exomes.r2.1.coverage.liftover_grch38.ht',
            'select': {'x10': 'over_10'}
        }
    },
    'gnomad_genome_coverage': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1/coverage/genomes/gnomad.genomes.r2.1.coverage.ht',
            'select': {'x10': '10'}
        },
        '38': {
            'path': 'gs://seqr-reference-data/gnomad_coverage/GRCh38/genomes/gnomad.genomes.r2.1.coverage.liftover_grch38.ht',
            'select': {'x10': 'over_10'}
        }
    },
    'gnomad_exomes': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1.1/ht/exomes/gnomad.exomes.r2.1.1.sites.ht',
            'custom_select': 'custom_gnomad_select'
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/gnomad/gnomad.exomes.r2.1.1.sites.liftover_grch38.ht',
            'custom_select': 'custom_gnomad_select'
        }
    },
    'gnomad_genomes': {
        '37': {
            'path': 'gs://gnomad-public/release/2.1.1/ht/genomes/gnomad.genomes.r2.1.1.sites.ht',
            'custom_select': 'custom_gnomad_select'
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/gnomad/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht',
            'custom_select': 'custom_gnomad_select'
        }
    },
    'exac': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/gnomad/ExAC.r1.sites.vep.ht',
            'select': {'AF_POPMAX': 'info.AF_POPMAX', 'AF': 'info.AF#', 'AC_Adj': 'info.AC_Adj#', 'AC_Het': 'info.AC_Het#',
                       'AC_Hom': 'info.AC_Hom#', 'AC_Hemi': 'info.AC_Hemi#', 'AN_Adj': 'info.AN_Adj'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/gnomad/ExAC.r1.sites.liftover.b38.ht',
            'select': {'AF_POPMAX': 'info.AF_POPMAX', 'AF': 'info.AF#', 'AC_Adj': 'info.AC_Adj#', 'AC_Het': 'info.AC_Het#',
                       'AC_Hom': 'info.AC_Hom#', 'AC_Hemi': 'info.AC_Hemi#', 'AN_Adj': 'info.AN_Adj'},
        },
    },
    'geno2mp': {
        '37': {
            'path': 'gs://seqr-reference-data/GRCh37/geno2mp/Geno2MP.variants.ht',
            'select': {'HPO_Count': 'info.HPO_CT'},
        },
        '38': {
            'path': 'gs://seqr-reference-data/GRCh38/geno2mp/Geno2MP.variants.liftover_38.ht',
            'select': {'HPO_Count': 'info.HPO_CT'}
        }
    }
}


def annotate_coverages(ht, coverage_dataset, reference_genome):
    """
    Annotates the hail table with the coverage dataset.
        '<coverage_dataset>': <over_10 field of the locus in the coverage dataset.>
    :param ht: hail table
    :param coverage_dataset: coverage dataset e.g. gnomad genomes or exomes coverage
    :param reference_genome: '37' or '38'
    :return: hail table with proper annotation
    """
    coverage_ht = hl.read_table(CONFIG[coverage_dataset][reference_genome]['path'])
    return ht.annotate(**{coverage_dataset: coverage_ht[ht.locus].over_10})

def custom_gnomad_select(ht):
    """
    Custom select for public gnomad dataset (which we did not generate). Extracts fields like
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

    selects['AF_POPMAX_OR_GLOBAL'] = ht.popmax[ht.globals.popmax_index_dict['gnomad']].AF
    selects['FAF_AF'] = ht.faf[ht.globals.popmax_index_dict['gnomad']].faf95
    selects['Hemi'] = hl.cond(ht.locus.in_autosome_or_par(),
                              0, ht.freq[ht.globals.freq_index_dict['gnomad_male']].AC)
    return selects

def get_select_fields(selects, base_ht):
    """
    Generic function that takes in a select config and base_ht and generates a
    select dict that is generated from traversing the base_ht and extracting the right
    annotation. If '#' is included at the end of a select field, the appropriate
    biallelic position will be selected (e.g. 'x#' -> x[base_ht.a_index-1].
    :param selects: mapping or list of selections
    :param base_ht: base_ht to traverse
    :return: select mapping from annotation name to base_ht annotation
    """
    select_fields = {}
    if selects is not None:
        if isinstance(selects, list):
            select_fields = { selection: base_ht[selection] for selection in selects }
        elif isinstance(selects, dict):
            for key, val in selects.items():
                # Grab the field and continually select it from the hail table.
                ht = base_ht
                for attr in val.split('.'):
                    # Select from multi-allelic list.
                    if attr.endswith('#'):
                        attr = attr[:-1]
                        ht = ht[attr][base_ht.a_index-1]
                    else:
                        ht = ht[attr]
                select_fields[key] = ht
    return select_fields

def get_ht(dataset, reference_genome):
    ' Returns the appropriate deduped hail table with selects applied.'
    config = CONFIG[dataset][reference_genome]
    base_ht = hl.read_table(config['path'])

    # 'select' and 'custom_select's to generate dict.
    select_fields = get_select_fields(config.get('select'), base_ht)
    if 'custom_select' in config:
        custom_select_fn_str = config['custom_select']
        select_fields = {**select_fields, **globals()[custom_select_fn_str](base_ht)}


    field_name = config.get('field_name') or dataset
    select_query = {
        field_name: hl.struct(**select_fields)
    }

    print(select_fields)
    return base_ht.select(**select_query).distinct()

def join_hts(datasets, coverage_datasets=[], reference_genome='37'):
    # Get a list of hail tables and combine into an outer join.
    hts = [get_ht(dataset, reference_genome) for dataset in datasets]
    joined_ht = reduce((lambda joined_ht, ht: joined_ht.join(ht, 'outer')), hts)

    # Annotate coverages.
    for coverage_dataset in coverage_datasets:
        joined_ht = annotate_coverages(joined_ht, coverage_dataset, reference_genome)

    # Add metadata, but also removes previous globals.
    joined_ht = joined_ht.select_globals(date=datetime.now().isoformat(),
                                         datasets=hl.set(datasets + coverage_datasets))
    joined_ht.describe()

    output_path = os.path.join(OUTPUT_TEMPLATE.format(genome_version=reference_genome))
    print('Writing to %s' % output_path)

    joined_ht.write(os.path.join(output_path))

def run(args):
    join_hts(['1kg', 'mpc', 'cadd', 'eigen', 'dbnsfp', 'topmed', 'primate_ai', 'splice_ai', 'exac',
              'gnomad_genomes', 'gnomad_exomes', 'geno2mp'],
             ['gnomad_genome_coverage', 'gnomad_exome_coverage'],
             args.build)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--build', help='Reference build, 37 or 38', choices=["37", "38"], required=True)
    args = parser.parse_args()

    run(args)
