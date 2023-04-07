import argparse
from datetime import datetime
from functools import reduce
import os

import hail as hl

from hail_scripts.reference_data.config import CONFIG
from hail_scripts.reference_data.constants import VERSION


OUTPUT_TEMPLATE = 'gs://seqr-reference-data/GRCh{genome_version}/' \
                  'all_reference_data/v2/combined_reference_data_grch{genome_version}-{version}.ht'

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
    print(f"Reading in {dataset}")
    base_ht = hl.read_table(config['path'])

    if config.get('filter'):
        base_ht = base_ht.filter(config['filter'](base_ht))

    # 'select' and 'custom_select's to generate dict.
    select_fields = get_select_fields(config.get('select'), base_ht)
    if 'custom_select' in config:
        select_fields = {**select_fields, **config['custom_select'](base_ht)}


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

    # Track the dataset we've added as well as the source path.
    included_dataset = {k: v[reference_genome]['path'] for k, v in CONFIG.items() if k in datasets + coverage_datasets}
    # Add metadata, but also removes previous globals.
    joined_ht = joined_ht.select_globals(date=datetime.now().isoformat(),
                                         datasets=hl.dict(included_dataset),
                                         version=VERSION)
    joined_ht.describe()
    return joined_ht


def run(args):
    hl._set_flags(no_whole_stage_codegen='1')  # hail 0.2.78 hits an error on the join, this flag gets around it
    joined_ht = join_hts(['cadd', '1kg', 'mpc', 'eigen', 'dbnsfp', 'topmed', 'primate_ai', 'splice_ai', 'exac',
              'gnomad_genomes', 'gnomad_exomes', 'geno2mp'],
             ['gnomad_genome_coverage', 'gnomad_exome_coverage'],
             args.build,)
    output_path = os.path.join(OUTPUT_TEMPLATE.format(genome_version=args.build, version=VERSION))
    print('Writing to %s' % output_path)
    joined_ht.write(os.path.join(output_path))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--build', help='Reference build, 37 or 38', choices=["37", "38"], required=True)
    args = parser.parse_args()

    run(args)
