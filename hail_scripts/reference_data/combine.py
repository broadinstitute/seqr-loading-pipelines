from datetime import datetime

import hail as hl
import pytz

from hail_scripts.reference_data.config import CONFIG

from v03_pipeline.lib.model import ReferenceDatasetCollection, ReferenceGenome


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
    if hasattr(ht, 'locus'):
        ht = ht.filter(
            hl.literal(reference_genome.standard_contigs).contains(ht.locus.contig),
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
        path=(config['source_path'] if 'custom_import' in config else config['path']),
        version=parse_version(ht, dataset, config),
        enums=hl.Struct(
            **config.get(
                'enum_select',
                hl.missing(hl.tstruct(hl.tstr, hl.tarray(hl.tstr))),
            ),
        ),
    )
    return ht.select(**{dataset: ht.row.drop(*ht.key)}).distinct()


def annotate_dataset_globals(joined_ht: hl.Table, dataset: str, dataset_ht: hl.Table):
    return joined_ht.select_globals(
        paths=joined_ht.paths.annotate(**{dataset: dataset_ht.index_globals().path}),
        versions=joined_ht.versions.annotate(
            **{dataset: dataset_ht.index_globals().version},
        ),
        enums=joined_ht.enums.annotate(**{dataset: dataset_ht.index_globals().enums}),
        date=datetime.now(tz=pytz.timezone('US/Eastern')).isoformat(),
    )


def join_hts(
    reference_dataset_collection: ReferenceDatasetCollection,
    reference_genome: ReferenceGenome,
):
    key_type = reference_dataset_collection.table_key_type(reference_genome)
    joined_ht = hl.Table.parallelize(
        [],
        key_type,
        key=key_type.fields,
        globals=hl.Struct(
            paths=hl.Struct(),
            versions=hl.Struct(),
            enums=hl.Struct(),
        ),
    )
    for dataset in reference_dataset_collection.datasets:
        dataset_ht = get_ht(dataset, reference_genome)
        joined_ht = joined_ht.join(dataset_ht, 'outer')
        joined_ht = annotate_dataset_globals(joined_ht, dataset, dataset_ht)
    return joined_ht


def update_existing_joined_hts(
    destination_path: str,
    dataset: str,
    reference_dataset_collection: ReferenceDatasetCollection,
    reference_genome: ReferenceGenome,
):
    joined_ht = hl.read_table(destination_path)
    dataset_ht = get_ht(dataset, reference_genome)
    joined_ht = joined_ht.drop(dataset)
    joined_ht = joined_ht.join(dataset_ht, 'outer')
    joined_ht = joined_ht.filter(
        hl.any(
            [
                ~hl.is_missing(joined_ht[dataset])
                for dataset in reference_dataset_collection.datasets
            ]
        ),
    )
    return annotate_dataset_globals(joined_ht, dataset, dataset_ht)
