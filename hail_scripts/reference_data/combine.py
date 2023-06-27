import functools
from datetime import datetime
from typing import List

import hail as hl
import pytz

from hail_scripts.reference_data.config import CONFIG

from v03_pipeline.lib.model import ReferenceGenome


def parse_version(ht: hl.Table, dataset: str, config: dict) -> hl.StringExpression:
    annotated_version = ht.globals.get('version', hl.missing(hl.tstr))
    config_version = config.get('version', hl.missing(hl.tstr))
    return (
        hl.case()
        .when(hl.is_missing(config_version), annotated_version)
        .when(hl.is_missing(annotated_version), config_version)
        .when(annotated_version == config_version, config_version)
        .or_error(
            f'found mismatching versions for dataset {dataset}, {config_version}, {hl.eval(annotated_version)}',
        )
    )


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
            expression = base_ht
            for attr in val.split('.'):
                # Select from multi-allelic list.
                if attr.endswith('#'):
                    expression = expression[attr[:-1]][base_ht.a_index - 1]
                else:
                    expression = expression[attr]
            # Parse float64s into float32s to save space!
            if expression.dtype == hl.tfloat64:
                expression = hl.float32(expression)
            select_fields[key] = expression
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
                # hard fail if a mapped key is present and has an unexpected value
                # but propagate missing values.
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


def get_ht(dataset: str, reference_genome: ReferenceGenome):
    config = CONFIG[dataset][reference_genome.v02_value]
    ht = (
        config['custom_import'](config['source_path'], reference_genome.v02_value)
        if 'custom_import' in config
        else hl.read_table(config['path'])
    )
    ht.filter(
        hl.set(reference_genome.standard_contigs).contains(ht.locus.contig)
    )
    ht = ht.filter(config['filter'](ht)) if 'filter' in config else ht
    ht = ht.select(
        **{
            **get_select_fields(config.get('select'), ht),
            **get_custom_select_fields(config.get('custom_select'), ht),
        },
    )
    ht = ht.transmute(**get_enum_select_fields(config.get('enum_select'), ht))
    ht = ht.select_globals(
        **{
            f'{dataset}_globals': hl.struct(
                path=config['source_path']
                if 'custom_import' in config
                else config['path'],
                version=parse_version(ht, dataset, config),
                enums=config.get(
                    'enum_select',
                    hl.missing(hl.tdict(hl.tstr, hl.tarray(hl.tstr))),
                ),
            ),
        },
    )
    return ht.select(**{dataset: ht.row.drop(*ht.key)}).distinct()


def update_joined_ht_globals(
    joined_ht,
):
    return joined_ht.annotate_globals(
        date=datetime.now(tz=pytz.timezone('US/Eastern')).isoformat(),
    )


def join_hts(datasets: List[str], reference_genome: ReferenceGenome):
    # Get a list of hail tables and combine into an outer join.
    hts = [get_ht(dataset, reference_genome) for dataset in datasets]
    joined_ht = functools.reduce(
        (lambda joined_ht, ht: joined_ht.join(ht, 'outer')),
        hts,
    )
    return update_joined_ht_globals(joined_ht)


def update_existing_joined_hts(
    destination_path: str,
    dataset: str,
    datasets: List[str],
    reference_genome: ReferenceGenome,
):
    joined_ht = hl.read_table(destination_path)
    dataset_ht = get_ht(dataset, reference_genome)
    joined_ht = joined_ht.drop(dataset, f'{dataset}_globals')
    joined_ht = joined_ht.join(dataset_ht, 'outer')
    joined_ht = joined_ht.filter(
        hl.any([~hl.is_missing(joined_ht[dataset]) for dataset in datasets]),
    )
    return update_joined_ht_globals(joined_ht)
