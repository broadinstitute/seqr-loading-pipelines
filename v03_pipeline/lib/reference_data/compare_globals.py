import dataclasses
import logging

import hail as hl

from v03_pipeline.lib.model import (
    DatasetType,
    ReferenceDatasetCollection,
    ReferenceGenome,
)
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    get_all_select_fields,
    get_ht_path,
    import_ht_from_config_path,
    parse_dataset_version,
)

logger = logging.getLogger(__name__)


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
            selects[dataset] = set(
                get_all_select_fields(dataset_ht, dataset_config).keys(),
            )
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
        enums = dict(rdc_globals_struct.enums)

        selects = {}
        for dataset in rdc.datasets(dataset_type):
            if dataset in ht.row:
                selects[dataset] = set(ht[dataset])
        return cls(paths, versions, enums, selects)


class GlobalsValidator:
    def __init__(
        self,
        ht1_globals: Globals,
        ht2_globals: Globals,
        reference_dataset_collection: ReferenceDatasetCollection,
        dataset_type: DatasetType,
    ):
        self.ht1_globals = ht1_globals
        self.ht2_globals = ht2_globals
        self.rdc = reference_dataset_collection
        self.dataset_type = dataset_type

    def get_datasets_to_update(self) -> list[str]:
        return [
            dataset
            for dataset in self.rdc.datasets(self.dataset_type)
            if not self._validate_globals_match(dataset)
        ]

    def _validate_globals_match(self, dataset: str) -> bool:
        results = []
        for field in dataclasses.fields(Globals):
            if field.name == 'selects':
                result = self._compare_selects(dataset)
            else:
                result = self.ht1_globals[field.name].get(dataset) == self.ht2_globals[
                    field.name
                ].get(dataset)
            if result is False:
                logger.info(f'{field.name} mismatch for {dataset}, {self.rdc.value}')
            results.append(result)
        return all(results)

    def _compare_selects(self, dataset: str) -> bool:
        ht1_selects = self.ht1_globals.selects.get(dataset)
        ht2_selects = self.ht2_globals.selects.get(dataset)
        if ht1_selects is None or ht2_selects is None:
            return False
        return len(ht1_selects.symmetric_difference(ht2_selects)) == 0
