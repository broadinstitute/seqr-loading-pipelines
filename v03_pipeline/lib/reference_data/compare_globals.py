import dataclasses

import hail as hl

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import (
    ReferenceGenome,
)
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    get_all_select_fields,
    get_enum_select_fields,
    import_ht_from_config_path,
)

logger = get_logger(__name__)


@dataclasses.dataclass
class Globals:
    paths: dict[str, str]
    versions: dict[str, str]
    enums: dict[str, dict[str, list[str]]]
    selects: dict[str, dict[str, hl.dtype]]

    def __getitem__(self, name: str):
        return getattr(self, name)

    @classmethod
    def from_dataset_configs(
        cls,
        reference_genome: ReferenceGenome,
        datasets: list[str],
    ):
        paths, versions, enums, selects = {}, {}, {}, {}
        for dataset in datasets:
            dataset_config = CONFIG[dataset][reference_genome.v02_value]
            dataset_ht = import_ht_from_config_path(
                dataset_config,
                dataset,
                reference_genome,
            )
            dataset_ht_globals = hl.eval(dataset_ht.globals)
            paths[dataset] = dataset_ht_globals.path
            versions[dataset] = dataset_ht_globals.version
            enums[dataset] = dict(dataset_ht_globals.enums)
            dataset_ht = dataset_ht.select(
                **get_all_select_fields(dataset_ht, dataset_config),
            )
            dataset_ht = dataset_ht.transmute(
                **get_enum_select_fields(dataset_ht, dataset_config),
            )
            selects[dataset] = {
                k: v.dtype
                for k, v in dict(dataset_ht.row).items()
                if k not in set(dataset_ht.key)
            }
        return cls(paths, versions, enums, selects)

    @classmethod
    def from_ht(
        cls,
        ht: hl.Table,
        datasets: list[str],
    ):
        rdc_globals_struct = hl.eval(ht.globals)
        paths = dict(rdc_globals_struct.paths)
        versions = dict(rdc_globals_struct.versions)
        # enums are nested structs
        enums = {k: dict(v) for k, v in rdc_globals_struct.enums.items() if k in paths}
        selects = {}
        for dataset in datasets:
            if dataset in ht.row:
                # NB: handle an edge case (mito high constraint) where we annotate a bool from the reference dataset collection
                selects[dataset] = (
                    {k: v.dtype for k, v in dict(ht[dataset]).items()}
                    if isinstance(ht[dataset], hl.StructExpression)
                    else {}
                )
        return cls(paths, versions, enums, selects)


def validate_selects_types(
    ht1_globals: Globals, ht2_globals: Globals, dataset: str
) -> None:
    # Assert that all shared annotations have identical types
    shared_selects = (
        ht1_globals['selects'][dataset].keys()
        & ht2_globals['selects'].get(dataset).keys()
    )
    mismatched_select_types = [
        (select, ht2_globals['selects'][dataset][select])
        for select in shared_selects
        if (
            ht1_globals['selects'][dataset][select]
            != ht2_globals['selects'][dataset][select]
        )
    ]
    if mismatched_select_types:
        msg = f'Unexpected field types detected in {dataset}: {mismatched_select_types}'
        raise ValueError(msg)


def get_datasets_to_update(
    ht1_globals: Globals,
    ht2_globals: Globals,
    validate_selects: bool = True,
) -> list[str]:
    datasets_to_update = set()
    for field in dataclasses.fields(Globals):
        if field.name == 'selects' and not validate_selects:
            continue
        datasets_to_update.update(
            ht1_globals[field.name].keys() ^ ht2_globals[field.name].keys(),
        )
        for dataset in ht1_globals[field.name].keys() & ht2_globals[field.name].keys():
            if field.name == 'selects':
                validate_selects_types(ht1_globals, ht2_globals, dataset)
            if ht1_globals[field.name][dataset] != ht2_globals[field.name][dataset]:
                logger.info(f'{field.name} mismatch for {dataset}')
                datasets_to_update.add(dataset)
    return sorted(datasets_to_update)
