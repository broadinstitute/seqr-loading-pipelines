import hail as hl
import luigi

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import ReferenceDatasetCollection
from v03_pipeline.lib.paths import valid_reference_dataset_collection_path
from v03_pipeline.lib.reference_data.compare_globals import (
    Globals,
    clinvar_versions_equal,
    get_datasets_to_update,
)
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    update_or_create_joined_ht,
)
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_update import BaseUpdateTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.validate_callset import ValidateCallsetTask

logger = get_logger(__name__)


@luigi.util.inherits(BaseLoadingRunParams)
class UpdatedReferenceDatasetCollectionTask(BaseUpdateTask):
    reference_dataset_collection = luigi.EnumParameter(enum=ReferenceDatasetCollection)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._datasets_to_update = []

    def requires(self) -> luigi.Task:
        # Though there is no explicit functional dependency between
        # validing the callset and updating the reference data, it's
        # a more user-friendly experience for the callset validation
        # to fail/succeed prior to attempting any
        # compute intensive work.
        #
        # Note that, if validation is disabled or skipped the task
        # still runs but is a no-op.
        return self.clone(ValidateCallsetTask)

    def complete(self) -> bool:
        self._datasets_to_update = []
        datasets = self.reference_dataset_collection.datasets(self.dataset_type)

        if not super().complete():
            logger.info('Creating a new reference dataset collection')
            self._datasets_to_update.extend(
                self.reference_dataset_collection.datasets(
                    self.dataset_type,
                ),
            )
            return False

        if any('clinvar' in d for d in datasets) and not clinvar_versions_equal(
            hl.read_table(self.output().path),
            self.reference_genome,
            self.dataset_type,
        ):
            datasets.remove('clinvar')
            self._datasets_to_update.append('clinvar')

        joined_ht_globals = Globals.from_ht(
            hl.read_table(self.output().path),
            datasets,
        )
        dataset_config_globals = Globals.from_dataset_configs(
            self.reference_genome,
            datasets,
        )
        self._datasets_to_update.extend(
            get_datasets_to_update(
                joined_ht_globals,
                dataset_config_globals,
            ),
        )
        logger.info(
            f'Datasets to update: {self._datasets_to_update} for {self.reference_dataset_collection}',
        )
        return not self._datasets_to_update

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            valid_reference_dataset_collection_path(
                self.reference_genome,
                self.dataset_type,
                self.reference_dataset_collection,
            ),
        )

    def initialize_table(self) -> hl.Table:
        key_type = self.reference_dataset_collection.table_key_type(
            self.reference_genome,
        )
        return hl.Table.parallelize(
            [],
            key_type,
            key=key_type.fields,
            globals=hl.Struct(
                paths=hl.Struct(),
                versions=hl.Struct(),
                enums=hl.Struct(),
            ),
        )

    def update_table(self, ht: hl.Table) -> hl.Table:
        return update_or_create_joined_ht(
            self.reference_dataset_collection,
            self.dataset_type,
            self.reference_genome,
            self._datasets_to_update,
            ht,
        )
