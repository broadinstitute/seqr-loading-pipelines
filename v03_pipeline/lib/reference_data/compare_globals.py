import logging

import hail as hl
from hail.utils import HailUserError

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    import_ht_from_config_path,
    parse_dataset_version,
)

logger = logging.getLogger(__name__)


def validate_joined_ht_globals_match_config(
    joined_ht: hl.Table,
    dataset: str,
    reference_genome: ReferenceGenome,
) -> bool:
    joined_ht_globals = joined_ht.index_globals()
    dataset_config = CONFIG[dataset][reference_genome.v02_value]
    checks = {
        'version': ht_version_matches_config(
            joined_ht_globals,
            dataset,
            dataset_config,
            reference_genome,
        ),
        'path': ht_path_matches_config(joined_ht_globals, dataset, dataset_config),
        'enum': ht_enums_match_config(joined_ht_globals, dataset, dataset_config),
        'select': ht_selects_match_config(joined_ht, dataset, dataset_config),
    }

    results = []
    for check, result in checks.items():
        evaluated_result = hl.eval(result)

        if evaluated_result is False:
            logger.info(f'{check} mismatch for {dataset}')

        results.append(evaluated_result)

    return all(results)


def ht_version_matches_config(
    joined_ht_globals: hl.StructExpression,
    dataset: str,
    dataset_config: dict,
    reference_genome: ReferenceGenome,
) -> hl.BooleanExpression:
    joined_ht_version = joined_ht_globals.versions.get(dataset)
    if joined_ht_version is None:
        return hl.bool(False)

    dataset_ht = import_ht_from_config_path(dataset_config, reference_genome)

    try:
        config_or_dataset_version = parse_dataset_version(
            dataset_ht,
            dataset,
            dataset_config,
        )
    except HailUserError as e:
        logger.warning(f'{e}. Please update the version in the config file.')
        return hl.bool(True)

    return hl.if_else(joined_ht_version == config_or_dataset_version, True, False)


def ht_path_matches_config(
    joined_ht_globals: hl.StructExpression,
    dataset: str,
    dataset_config: dict,
) -> hl.BooleanExpression:
    joined_ht_path = joined_ht_globals.paths.get(dataset)
    if joined_ht_path is None:
        return hl.bool(False)

    config_path = (
        dataset_config['source_path']
        if 'custom_import' in dataset_config
        else dataset_config['path']
    )
    return hl.if_else(joined_ht_path == config_path, True, False)


def ht_enums_match_config(
    joined_ht_globals: hl.StructExpression,
    dataset: str,
    dataset_config: dict,
) -> hl.BooleanExpression:
    joined_ht_enums = hl.eval(joined_ht_globals.enums.get(dataset, hl.Struct()))
    config_enums = hl.eval(hl.struct(**dataset_config.get('enum_select', {})))
    return hl.if_else(joined_ht_enums == config_enums, True, False)


def ht_selects_match_config(
    joined_ht: hl.Table,
    dataset: str,
    dataset_config: dict,
) -> hl.BooleanExpression:
    joined_ht_selects = set(joined_ht[dataset])
    raw_config_selects = dataset_config.get('select', {})
    config_selects = (
        set(raw_config_selects)
        if isinstance(raw_config_selects, list)
        else set(raw_config_selects.keys())
    )

    if 'custom_select' in dataset_config:
        # TODO refactor these
        config_selects.update(set(dataset_config.get('custom_select_keys')))

    return hl.if_else(
        len(config_selects.symmetric_difference(joined_ht_selects)) == 0,
        True,
        False,
    )
