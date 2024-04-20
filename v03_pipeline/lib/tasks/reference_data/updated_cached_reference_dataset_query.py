import hail as hl
import luigi

from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    Env,
    ReferenceDatasetCollection,
)
from v03_pipeline.lib.paths import (
    valid_cached_reference_dataset_query_path,
    valid_reference_dataset_collection_path,
)
from v03_pipeline.lib.reference_data.compare_globals import (
    Globals,
    get_datasets_to_update,
)
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    get_ht_path,
    import_ht_from_config_path,
)
from v03_pipeline.lib.tasks.base.base_write import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, HailTableTask
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection import (
    UpdatedReferenceDatasetCollectionTask,
)

logger = get_logger(__name__)


class UpdatedCachedReferenceDatasetQuery(BaseWriteTask):
    crdq = luigi.EnumParameter(enum=CachedReferenceDatasetQuery)

    def complete(self) -> bool:
        if not super().complete():
            logger.info(
                f'UpdatedCachedReferenceDatasetQuery: {self.output().path} does not exist',
            )
            return False

        datasets_to_check = [self.crdq.dataset(self.dataset_type)]
        crdq_globals = Globals.from_ht(
            hl.read_table(self.output().path),
            datasets_to_check,
        )
        dataset_config_globals = Globals.from_dataset_configs(
            self.reference_genome,
            datasets_to_check,
        )
        return not get_datasets_to_update(
            crdq_globals,
            dataset_config_globals,
            validate_selects=False,
        )

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            valid_cached_reference_dataset_query_path(
                self.reference_genome,
                self.dataset_type,
                self.crdq,
            ),
        )

    def requires(self) -> luigi.Task:
        if Env.REFERENCE_DATA_AUTO_UPDATE and not self.crdq.query_raw_dataset:
            return UpdatedReferenceDatasetCollectionTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                ReferenceDatasetCollection.COMBINED,
            )
        if self.crdq.query_raw_dataset:
            return HailTableTask(
                get_ht_path(
                    CONFIG[self.crdq.dataset(self.dataset_type)][
                        self.reference_genome.v02_value
                    ],
                ),
            )
        return HailTableTask(
            valid_reference_dataset_collection_path(
                self.reference_genome,
                self.dataset_type,
                ReferenceDatasetCollection.COMBINED,
            ),
        )

    def create_table(self) -> hl.Table:
        dataset: str = self.crdq.dataset(self.dataset_type)
        if self.crdq.query_raw_dataset:
            query_ht = import_ht_from_config_path(
                CONFIG[dataset][self.reference_genome.v02_value],
                dataset,
                self.reference_genome,
            )
        else:
            query_ht = hl.read_table(
                valid_reference_dataset_collection_path(
                    self.reference_genome,
                    self.dataset_type,
                    ReferenceDatasetCollection.COMBINED,
                ),
            )
        ht = self.crdq.query(
            query_ht,
            dataset_type=self.dataset_type,
            reference_genome=self.reference_genome,
        )
        return ht.select_globals(
            paths=hl.Struct(
                **{
                    dataset: query_ht.index_globals().path
                    if self.crdq.query_raw_dataset
                    else query_ht.index_globals().paths[dataset],
                },
            ),
            versions=hl.Struct(
                **{
                    dataset: query_ht.index_globals().version
                    if self.crdq.query_raw_dataset
                    else query_ht.index_globals().versions[dataset],
                },
            ),
            enums=hl.Struct(
                **{
                    dataset: query_ht.index_globals().enums
                    if self.crdq.query_raw_dataset
                    else query_ht.index_globals().enums[dataset],
                },
            ),
        )
