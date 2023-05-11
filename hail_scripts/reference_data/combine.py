import functools
from datetime import datetime
from typing import List

import hail as hl

from hail_scripts.reference_data.config import CONFIG


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
    if selects is None:
        return select_fields
    if isinstance(selects, list):
        select_fields = {selection: base_ht[selection] for selection in selects}
    elif isinstance(selects, dict):
        for key, val in selects.items():
            # Grab the field and continually select it from the hail table.
            ht = base_ht
            for attr in val.split('.'):
                # Select from multi-allelic list.
                if attr.endswith('#'):
                    ht = ht[attr[:-1]][base_ht.a_index - 1]
                else:
                    ht = ht[attr]
            select_fields[key] = ht
    return select_fields


def get_custom_select_fields(custom_select, ht):
    if custom_select is None:
        return {}
    return custom_select(ht)


def get_enum_select_fields(enum_selects, ht):
    enum_select_fields = {}
    if enum_selects is None:
        return enum_select_fields
    for field_name, values in enum_selects.items():
        lookup = hl.dict(
            hl.enumerate(values, index_first=False).extend(
                # NB: adding missing values here allows us to
                # hard fail if a mapped key is present but an unexpected value
                # but also propagate missing values.
                [(hl.missing(hl.tstr), hl.missing(hl.tint32))],
            ),
        )
        # NB: this conditioning on type is "outside" the hail expression context.
        if (
            isinstance(ht[field_name].dtype, (hl.tarray, hl.tset))
            and ht[field_name].dtype.element_type == hl.tstr
        ):
            enum_select_fields[f'{field_name}_ids'] = ht[field_name].map(
                lambda x: lookup[x],  # noqa: B023
            )
        else:
            enum_select_fields[f'{field_name}_id'] = lookup[ht[field_name]]
    return enum_select_fields


def get_ht(dataset: str, reference_genome: str):
    config = CONFIG[dataset][reference_genome]
    field_name = config.get('field_name') or dataset
    ht = hl.read_table(config['path'])
    ht = ht.filter(config['filter'](ht)) if 'filter' in config else ht
    ht = ht.select(
        **{
            **get_select_fields(config.get('select'), ht),
            **get_custom_select_fields(config.get('custom_select'), ht),
        },
    )
    ht = ht.transmute(**get_enum_select_fields(config.get('enum_select'), ht))
    return ht.select(**{field_name: ht.row.drop(*ht.key)}).distinct()


def update_joined_ht_globals(
    joined_ht,
    datasets,
    version,
    reference_genome,
):
    # Track the dataset we've added as well as the source path.
    included_dataset = {
        k: v[reference_genome]['path'] for k, v in CONFIG.items() if k in datasets
    }
    enum_definitions = {
        k: {enum_field_name: enum_values}
        for k, v in CONFIG.items()
        if k in datasets
        if 'enum_select' in v[reference_genome]
        for enum_field_name, enum_values in v[reference_genome]['enum_select'].items()
    }
    # Add metadata, but also removes previous globals.
    return joined_ht.select_globals(
        date=datetime.now().isoformat(),
        datasets=hl.dict(included_dataset),
        version=version,
        enum_definitions=hl.dict(enum_definitions),
    )


def join_hts(datasets, version, reference_genome='37'):
    # Get a list of hail tables and combine into an outer join.
    hts = [
        get_ht(dataset, reference_genome)
        for dataset in datasets
        if 'coverage' not in dataset
    ]
    joined_ht = functools.reduce(
        (lambda joined_ht, ht: joined_ht.join(ht, 'outer')),
        hts,
    )

    # NB: coverage datasets are keyed by locus rather than locus
    # and alleles, so we cannot join.  Instead we annotate w/ locus as
    # as the key.
    coverage_hts = [
        (dataset, get_ht(dataset, reference_genome))
        for dataset in datasets
        if 'coverage' in dataset
    ]
    for dataset, coverage_ht in coverage_hts:
        joined_ht.annotate(dataset=coverage_ht[coverage_ht.locus][dataset])

    joined_ht = update_joined_ht_globals(
        joined_ht,
        datasets,
        version,
        reference_genome,
    )
    joined_ht.describe()
    return joined_ht


def update_existing_joined_hts(
    destination_path: str,
    dataset: str,
    datasets: List[str],
    version: str,
    genome_version: str,
):
    destination_ht = hl.read_table(destination_path)
    dataset_ht = get_ht(dataset, genome_version)
    destination_ht = (
        destination_ht.drop(dataset)
        .join(dataset_ht, 'outer')
        .filter(
            hl.any([~hl.is_missing(destination_ht[dataset]) for dataset in datasets]),
        )
    )
    return update_joined_ht_globals(destination_ht, dataset, version, genome_version)
