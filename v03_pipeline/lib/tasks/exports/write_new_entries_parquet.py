import luigi
import luigi.util

from v03_pipeline.lib.paths import (
    new_entries_parquet_path,
)
from v03_pipeline.lib.reference_datasets.reference_dataset import ReferenceDatasetQuery
from v03_pipeline.lib.tasks.base.base_loading_run_params import (
    BaseLoadingRunParams,
)
from v03_pipeline.lib.tasks.base.base_write_parquet import BaseWriteParquetTask
from v03_pipeline.lib.tasks.files import GCSorLocalTarget
from v03_pipeline.lib.tasks.reference_data.updated_reference_dataset_query import (
    UpdatedReferenceDatasetQueryTask,
)
from v03_pipeline.lib.tasks.update_new_variants_with_caids import (
    UpdateNewVariantsWithCAIDsTask,
)
from v03_pipeline.lib.tasks.write_new_variants_table import WriteNewVariantsTableTask
from v03_pipeline.lib.tasks.write_remapped_and_subsetted_callset import (
    WriteRemappedAndSubsettedCallsetTask,
)


@luigi.util.inherits(BaseLoadingRunParams)
class WriteNewEntriesParquetTask(BaseWriteParquetTask):
    def output(self) -> luigi.Target:
        return GCSorLocalTarget(
            new_entries_parquet_path(
                self.reference_genome,
                self.dataset_type,
                self.run_id,
            ),
        )

    def requires(self) -> list[luigi.Task]:
        return {
            'annotations': (
                self.clone(UpdateNewVariantsWithCAIDsTask)
                if self.dataset_type.should_send_to_allele_registry
                else self.clone(WriteNewVariantsTableTask)
            ),
            'high_af_variants': self.clone(
                UpdatedReferenceDatasetQueryTask,
                reference_dataset_query=ReferenceDatasetQuery.high_af_variants,
            ),
            'remapped_and_subsetted_callsets': [
                self.clone(
                    WriteRemappedAndSubsettedCallsetTask,
                    project_i=i,
                )
                for i in range(len(self.project_guids))
            ],
        }

    def run(self) -> None:
        pass
