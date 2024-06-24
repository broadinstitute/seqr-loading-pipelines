from datetime import datetime
from types import FunctionType

import hail as hl
import pytz

from v03_pipeline.lib.misc.io import checkpoint
from v03_pipeline.lib.misc.nested_field import parse_nested_field
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.reference_data.config import CONFIG


def update_or_create_joined_ht(
    reference_dataset_collection: ReferenceDatasetCollection,
    dataset_type: DatasetType,
    reference_genome: ReferenceGenome,
    datasets: list[str],
    joined_ht: hl.Table,
) -> hl.Table:
    for dataset in datasets:
        # Drop the dataset if it exists.
        if dataset in joined_ht.row:
            joined_ht = joined_ht.drop(dataset)
            joined_ht = joined_ht.annotate_globals(
                paths=joined_ht.paths.drop(dataset),
                versions=joined_ht.versions.drop(dataset),
                enums=joined_ht.enums.drop(dataset),
            )

        # Handle cases where a dataset has been dropped OR renamed.
        if dataset not in CONFIG:
            continue

        # Join the new one!
        hl._set_flags(use_new_shuffle=None, no_whole_stage_codegen='1')  # noqa: SLF001
        dataset_ht = get_dataset_ht(dataset, reference_genome)
        dataset_ht, _ = checkpoint(dataset_ht)
        hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001
        joined_ht = joined_ht.join(dataset_ht, 'outer')
        joined_ht = annotate_dataset_globals(joined_ht, dataset, dataset_ht)

    return joined_ht.filter(
        hl.any(
            [
                ~hl.is_missing(joined_ht[dataset])
                for dataset in reference_dataset_collection.datasets(dataset_type)
            ],
        ),
    )


def get_dataset_ht(
    dataset: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    config = CONFIG[dataset][reference_genome.v02_value]
    ht = import_ht_from_config_path(config, dataset, reference_genome)
    if hasattr(ht, 'locus'):
        ht = ht.filter(
            hl.set(reference_genome.standard_contigs).contains(ht.locus.contig),
        )

    ht = ht.filter(config['filter'](ht)) if 'filter' in config else ht
    ht = ht.select(**get_all_select_fields(ht, config))
    ht = ht.transmute(**get_enum_select_fields(ht, config))
    return ht.select(**{dataset: ht.row.drop(*ht.key)}).distinct()


def get_ht_path(config: dict) -> str:
    return config['source_path'] if 'custom_import' in config else config['path']


def import_ht_from_config_path(
    config: dict,
    dataset: str,
    reference_genome: ReferenceGenome,
) -> hl.Table:
    path = get_ht_path(config)
    ht = (
        config['custom_import'](path, reference_genome)
        if 'custom_import' in config
        else hl.read_table(path)
    )
    return ht.annotate_globals(
        path=path,
        version=parse_dataset_version(ht, dataset, config),
        enums=hl.Struct(
            **config.get(
                'enum_select',
                hl.missing(hl.tstruct(hl.tstr, hl.tarray(hl.tstr))),
            ),
        ),
    )


def get_select_fields(selects: list | dict | None, base_ht: hl.Table) -> dict:
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
            expression = parse_nested_field(base_ht, val)
            # Parse float64s into float32s to save space!
            if expression.dtype == hl.tfloat64:
                expression = hl.float32(expression)
            select_fields[key] = expression
    return select_fields


def get_custom_select_fields(custom_select: FunctionType | None, ht: hl.Table) -> dict:
    if custom_select is None:
        return {}
    return custom_select(ht)


def get_all_select_fields(
    ht: hl.Table,
    config: dict,
) -> dict:
    return {
        **get_select_fields(config.get('select'), ht),
        **get_custom_select_fields(config.get('custom_select'), ht),
    }


def get_enum_select_fields(ht: hl.Table, config: dict) -> dict:
    enum_selects = config.get('enum_select')
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
            isinstance(ht[field_name].dtype, hl.tarray | hl.tset)
            and ht[field_name].dtype.element_type == hl.tstr
        ):
            enum_select_fields[f'{field_name}_ids'] = ht[field_name].map(
                lambda x: lookup[x],  # noqa: B023
            )
        else:
            enum_select_fields[f'{field_name}_id'] = lookup[ht[field_name]]
    return enum_select_fields


def parse_dataset_version(
    ht: hl.Table,
    dataset: str,
    config: dict,
) -> hl.StringExpression:
    annotated_version = ht.globals.get('version', hl.missing(hl.tstr))
    config_version = config.get('version', hl.missing(hl.tstr))
    return (
        hl.case()
        .when(hl.is_missing(config_version), annotated_version)
        .when(hl.is_missing(annotated_version), config_version)
        .when(annotated_version == config_version, config_version)
        .or_error(
            hl.format(
                'found mismatching versions for dataset %s. config version: %s, ht version: %s',
                dataset,
                config_version,
                annotated_version,
            ),
        )
    )


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
    reference_genome: ReferenceGenome,
    dataset_type: DatasetType,
    reference_dataset_collection: ReferenceDatasetCollection,
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
    for dataset in reference_dataset_collection.datasets(dataset_type):
        hl._set_flags(use_new_shuffle=None, no_whole_stage_codegen='1')  # noqa: SLF001
        dataset_ht = get_dataset_ht(dataset, reference_genome)
        dataset_ht, _ = checkpoint(dataset_ht)
        hl._set_flags(use_new_shuffle='1', no_whole_stage_codegen='1')  # noqa: SLF001
        joined_ht = joined_ht.join(dataset_ht, 'outer')
        joined_ht = annotate_dataset_globals(joined_ht, dataset, dataset_ht)
    return joined_ht
