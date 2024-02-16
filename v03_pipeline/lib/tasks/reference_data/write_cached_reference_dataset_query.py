import hail as hl
import luigi

from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
    Env,
    ReferenceDatasetCollection,
)
from v03_pipeline.lib.paths import (
    valid_cached_reference_dataset_query_path,
    valid_reference_dataset_collection_path,
)
from v03_pipeline.lib.reference_data.config import CONFIG
from v03_pipeline.lib.reference_data.dataset_table_operations import (
    get_ht_path,
    import_ht_from_config_path,
)
from v03_pipeline.lib.tasks.base.base_write_task import BaseWriteTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget, HailTableTask
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_collection import UpdatedReferenceDatasetCollectionTask


class WriteCachedReferenceDatasetQuery(BaseWriteTask):
    crdq = luigi.EnumParameter(enum=CachedReferenceDatasetQuery)

    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            valid_cached_reference_dataset_query_path(
                self.reference_genome,
                self.dataset_type,
                self.query,
            ),
        )

    def requires(self) -> luigi.Task:
        if Env.REFERENCE_DATA_AUTO_UPDATE:
            return UpdatedReferenceDatasetCollectionTask(
                self.reference_genome,
                self.dataset_type,
                self.sample_type,
                ReferenceDatasetCollection.COMBINED,
            )
        if self.crdq.dataset:
            return HailTableTask(
                get_ht_path(CONFIG[self.crdq.reference_dataset][self.reference_genome.v02_value]),
            )
        return HailTableTask(
            valid_reference_dataset_collection_path(
                self.reference_genome,
                self.dataset_type,
                ReferenceDatasetCollection.COMBINED,
            ),
        )

    def create_table(self) -> hl.Table:
        if self.crdq.reference_dataset:
            ht = import_ht_from_config_path(CONFIG[self.crdq.reference_dataset][self.reference_genome.v02_value], self.reference_genome)
        else:
            ht = hl.read_table(
                valid_reference_dataset_collection_path(
                    self.reference_genome,
                    self.dataset_type,
                    ReferenceDatasetCollection.COMBINED,
                ),
            )
        return self.crdq.query(ht, dataset_type=self.dataset_type, reference_genome=self.reference_genome)
