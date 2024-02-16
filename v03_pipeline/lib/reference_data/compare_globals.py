import dataclasses

import hail as hl

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    get_all_select_fields,
    get_enum_select_fields,
    get_ht_path,
    import_ht_from_config_path,
    parse_dataset_version,
)

logger = get_logger(__name__)


@dataclasses.dataclass
class Globals:
    paths: dict[str]
    versions: dict[str]
    enums: dict[str, dict[str, list[str]]]
    selects: dict[str, set[str]]

    def __getitem__(self, name: str):
        return getattr(self, name)

    @classmethod
    def from_dataset_configs(
        cls,
        rdc: ReferenceDatasetCollection,
        dataset_type: DatasetType,
        reference_genome: ReferenceGenome,
    ):
        paths, versions, enums, selects = {}, {}, {}, {}
        for dataset in rdc.datasets(dataset_type):
            dataset_config = CONFIG[dataset][reference_genome.v02_value]
            dataset_ht = import_ht_from_config_path(dataset_config, reference_genome)

            paths[dataset] = get_ht_path(dataset_config)
            versions[dataset] = hl.eval(
                parse_dataset_version(
                    dataset_ht,
                    dataset,
                    dataset_config,
                ),
            )
            enums[dataset] = dataset_config.get('enum_select', {})
            dataset_ht = dataset_ht.select(
                **get_all_select_fields(dataset_ht, dataset_config)
            )
            dataset_ht = dataset_ht.transmute(
                **get_enum_select_fields(dataset_ht, dataset_config)
            )
            selects[dataset] = set(dataset_ht.row) - set(dataset_ht.key)
        return cls(paths, versions, enums, selects)

    @classmethod
    def from_ht(
        cls,
        ht: hl.Table,
        rdc: ReferenceDatasetCollection,
        dataset_type: DatasetType,
    ):
        rdc_globals_struct = hl.eval(ht.globals)
        paths = dict(rdc_globals_struct.paths)
        versions = dict(rdc_globals_struct.versions)
        # enums are nested structs
        enums = {k: dict(v) for k, v in rdc_globals_struct.enums.items()}

        selects = {}
        for dataset in rdc.datasets(dataset_type):
            if dataset in ht.row:
                # NB: handle an edge case (mito high constraint) where we annotate a bool from the reference dataset collection
                selects[dataset] = (
                    set(ht[dataset])
                    if isinstance(ht[dataset], hl.StructExpression)
                    else set()
                )
        return cls(paths, versions, enums, selects)


def get_datasets_to_update(
    rdc: ReferenceDatasetCollection,
    ht1_globals: Globals,
    ht2_globals: Globals,
    dataset_type: DatasetType,
) -> list[str]:
    return [
        dataset
        for dataset in rdc.datasets(dataset_type)
        if not validate_globals_match(rdc, ht1_globals, ht2_globals, dataset)
    ]


def validate_globals_match(
    rdc: ReferenceDatasetCollection,
    ht1_globals: Globals,
    ht2_globals: Globals,
    dataset: str,
) -> bool:
    results = []
    for field in dataclasses.fields(Globals):
        result = ht1_globals[field.name].get(dataset) == ht2_globals[field.name].get(
            dataset,
        )
        if result is False:
            logger.info(f'{field.name} mismatch for {dataset}, {rdc.value}')
        results.append(result)
    return all(results)
