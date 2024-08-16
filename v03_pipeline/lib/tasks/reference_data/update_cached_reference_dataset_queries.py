import luigi
import luigi.util

from v03_pipeline.lib.model import (
    CachedReferenceDatasetQuery,
)
from v03_pipeline.lib.tasks.base.base_loading_pipeline_params import (
    BaseLoadingPipelineParams,
)
from v03_pipeline.lib.tasks.reference_data.updated_cached_reference_dataset_query import (
    UpdatedCachedReferenceDatasetQuery,
)


@luigi.util.inherits(BaseLoadingPipelineParams)
class UpdateCachedReferenceDatasetQueries(luigi.Task):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checked_for_tasks = False
        self.dynamic_crdq_tasks = set()

    def complete(self) -> bool:
        return self.checked_for_tasks

    def run(self):
        self.checked_for_tasks = True
        for crdq in CachedReferenceDatasetQuery.for_reference_genome_dataset_type(
            self.reference_genome,
            self.dataset_type,
        ):
            self.dynamic_crdq_tasks.add(
                UpdatedCachedReferenceDatasetQuery(
                    **self.param_kwargs,
                    crdq=crdq,
                ),
            )
        yield self.dynamic_crdq_tasks
