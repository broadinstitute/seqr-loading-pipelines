import logging
from dataclasses import dataclass

import hail as hl

from v03_pipeline.lib.model import ReferenceGenome
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    get_all_select_fields,
    get_ht_path,
    import_ht_from_config_path,
    parse_dataset_version,
)

logger = logging.getLogger(__name__)


@dataclass
class ReferenceDataGlobals:
    paths: dict[str]
    versions: dict[str]
    enums: dict[str, dict[str, list[str]]]

    def __init__(self, globals_struct: hl.Struct):
        self.paths = self._struct_to_dict(globals_struct.paths)
        self.versions = self._struct_to_dict(globals_struct.versions)
        self.enums = self._struct_to_dict(globals_struct.enums)

    def _struct_to_dict(self, struct: hl.Struct) -> dict:
        result_dict = {}
        for field in struct:
            if isinstance(struct[field], hl.Struct):
                result_dict[field] = self._struct_to_dict(struct[field])
            else:
                result_dict[field] = struct[field]
        return result_dict


def get_datasets_to_update(
    joined_ht: hl.Table,
    datasets: list[str],
    reference_genome: ReferenceGenome,
) -> list[str]:
    datasets_to_update = []
    for dataset in datasets:
        if dataset not in joined_ht.row:
            datasets_to_update.append(dataset)
            continue

        if not validate_joined_ht_globals_match_config(
            joined_ht,
            dataset,
            reference_genome,
        ):
            datasets_to_update.append(dataset)
    return datasets_to_update


def validate_joined_ht_globals_match_config(
    joined_ht: hl.Table,
    dataset: str,
    reference_genome: ReferenceGenome,
) -> bool:
    joined_ht_globals = ReferenceDataGlobals(hl.eval(joined_ht.index_globals()))
    dataset_config = CONFIG[dataset][reference_genome.v02_value]
    dataset_ht = import_ht_from_config_path(dataset_config, reference_genome)
    checks = {
        'version': ht_version_matches_config(
            joined_ht_globals,
            dataset,
            dataset_config,
            dataset_ht,
        ),
        'path': ht_path_matches_config(joined_ht_globals, dataset, dataset_config),
        'enum': ht_enums_match_config(joined_ht_globals, dataset, dataset_config),
        'select': ht_selects_match_config(
            joined_ht,
            dataset,
            dataset_config,
            dataset_ht,
        ),
    }

    results = []
    for check, result in checks.items():
        if result is False:
            logger.info(f'{check} mismatch for {dataset}')
        results.append(result)
    return all(results)


def ht_version_matches_config(
    joined_ht_globals: ReferenceDataGlobals,
    dataset: str,
    dataset_config: dict,
    dataset_ht: hl.Table,
) -> bool:
    joined_ht_version = joined_ht_globals.versions.get(dataset)
    if joined_ht_version is None:
        return False

    config_or_dataset_version = hl.eval(
        parse_dataset_version(
            dataset_ht,
            dataset,
            dataset_config,
        ),
    )
    return joined_ht_version == config_or_dataset_version


def ht_path_matches_config(
    joined_ht_globals: ReferenceDataGlobals,
    dataset: str,
    dataset_config: dict,
) -> bool:
    joined_ht_path = joined_ht_globals.paths.get(dataset)
    if joined_ht_path is None:
        return False

    config_path = get_ht_path(dataset_config)
    return joined_ht_path == config_path


def ht_enums_match_config(
    joined_ht_globals: ReferenceDataGlobals,
    dataset: str,
    dataset_config: dict,
) -> bool:
    joined_ht_enums = joined_ht_globals.enums.get(dataset, {})
    config_enums = dataset_config.get('enum_select', {})
    return joined_ht_enums == config_enums


def ht_selects_match_config(
    joined_ht: hl.Table,
    dataset: str,
    dataset_config: dict,
    dataset_ht: hl.Table,
) -> bool:
    joined_ht_selects = set(joined_ht[dataset])
    config_selects = set(get_all_select_fields(dataset_ht, dataset_config).keys())
    return len(config_selects.symmetric_difference(joined_ht_selects)) == 0
